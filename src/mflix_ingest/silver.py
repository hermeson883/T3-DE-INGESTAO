"""Camada Silver — bonus +4.

- deduplicacao por _source_id mantendo o registro mais recente (_ingestion_timestamp)
- movies: flatten de imdb/tomatoes/awards + explode de cast/genres/directors/...
- comments: dedup + tipagem de `date`
Le a Bronze via a coluna VARIANT (`body_variant`) — nenhuma regra de negocio,
apenas normalizacao estrutural.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import PipelineConfig
from .utils import get_logger

_log = get_logger("mflix_ingest.silver")


class SilverBuilder:
    def __init__(self, spark: SparkSession, cfg: PipelineConfig):
        self.spark = spark
        self.cfg = cfg
        self.t = cfg.target

    # ------------------------------------------------------------------ #
    def _latest(self, collection: str) -> DataFrame:
        """1 linha por _source_id — a mais recente (latest record)."""
        src = self.spark.table(self.t.bronze_table(collection))
        w = Window.partitionBy("_source_id").orderBy(
            F.col("_ingestion_timestamp").desc(), F.col("_source_hash").desc()
        )
        return (
            src.withColumn("_rn", F.row_number().over(w))
            .where("_rn = 1")
            .drop("_rn")
        )

    # ------------------------------------------------------------------ #
    def build_movies(self) -> None:
        base = self._latest("movies").select(
            "_source_id", "_ingestion_id", "_ingestion_timestamp", "body_variant",
        )
        v = "body_variant"
        movies = base.select(
            F.col("_source_id"),
            F.expr(f"try_cast({v}:title as string)").alias("title"),
            F.expr(f"try_cast({v}:year as int)").alias("year"),
            F.expr(f"try_cast({v}:rated as string)").alias("rated"),
            F.expr(f"try_cast({v}:runtime as int)").alias("runtime"),
            F.expr(f"try_cast({v}:type as string)").alias("type"),
            F.expr(f"try_cast({v}:plot as string)").alias("plot"),
            F.expr(f"to_timestamp(try_cast({v}:released as string))").alias("released"),
            F.expr(f"try_cast({v}:lastupdated as string)").alias("lastupdated"),
            F.expr(f"try_cast({v}:imdb:rating as double)").alias("imdb_rating"),
            F.expr(f"try_cast({v}:imdb:votes as long)").alias("imdb_votes"),
            F.expr(f"try_cast({v}:imdb:id as long)").alias("imdb_id"),
            F.expr(f"try_cast({v}:tomatoes:viewer:rating as double)").alias("tomatoes_viewer_rating"),
            F.expr(f"try_cast({v}:tomatoes:viewer:numReviews as long)").alias("tomatoes_viewer_reviews"),
            F.expr(f"try_cast({v}:tomatoes:critic:rating as double)").alias("tomatoes_critic_rating"),
            F.expr(f"try_cast({v}:awards:wins as int)").alias("awards_wins"),
            F.expr(f"try_cast({v}:awards:nominations as int)").alias("awards_nominations"),
            F.expr(f"try_cast({v}:num_mflix_comments as int)").alias("num_mflix_comments"),
            F.col("_ingestion_id"),
            F.col("_ingestion_timestamp"),
        )
        self._write(movies, "movies")

        # arrays -> tabelas filhas (explode com posicao preservada)
        for arr in self.cfg.silver_movies_arrays:
            child = (
                base.select(
                    "_source_id",
                    F.posexplode_outer(
                        F.expr(f"try_cast({v}:{arr} as array<string>)")
                    ).alias("position", "value"),
                )
                .where(F.col("value").isNotNull())
                .dropDuplicates(["_source_id", "value"])
            )
            self._write(child, f"movies_{arr}")

    def build_comments(self) -> None:
        v = "body_variant"
        base = self._latest("comments")
        comments = base.select(
            F.col("_source_id"),
            F.expr(f"try_cast({v}:name as string)").alias("name"),
            F.expr(f"try_cast({v}:email as string)").alias("email"),
            F.expr(f"try_cast({v}:movie_id as string)").alias("movie_id"),
            F.expr(f"try_cast({v}:text as string)").alias("text"),
            F.expr(f"to_timestamp(try_cast({v}:date as string))").alias("date"),
            F.col("_ingestion_id"),
            F.col("_ingestion_timestamp"),
        )
        self._write(comments, "comments")

    def build_users(self) -> None:
        v = "body_variant"
        base = self._latest("users")
        users = base.select(
            F.col("_source_id"),
            F.expr(f"try_cast({v}:name as string)").alias("name"),
            F.expr(f"try_cast({v}:email as string)").alias("email"),
            F.col("_ingestion_id"),
            F.col("_ingestion_timestamp"),
        )
        self._write(users, "users")

    # ------------------------------------------------------------------ #
    def build_all(self) -> list[str]:
        built: list[str] = []
        for name, fn in (
            ("movies", self.build_movies),
            ("comments", self.build_comments),
            ("users", self.build_users),
        ):
            if self.spark.catalog.tableExists(self.t.bronze_table(name)):
                fn()
                built.append(name)
            else:
                _log.warning("bronze.%s inexistente — Silver pulada", name)
        return built

    def _write(self, df: DataFrame, name: str) -> None:
        fqn = self.t.silver_table(name)
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(fqn))
        _log.info("silver <- %s (%d linhas)", fqn, df.count())