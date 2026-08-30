"""Camada Silver — bonus +4.

- deduplicacao por _source_id mantendo o registro mais recente (latest record)
- movies: flatten de imdb/tomatoes/awards + explode de cast/genres/directors/...
- comments/users: dedup + tipagem

Le a Bronze via `body_json` (STRING) com `get_json_object` / `from_json` —
nenhuma regra de negocio, apenas normalizacao estrutural e tipagem.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .config import PipelineConfig
from .utils import get_logger

_log = get_logger("mflix_ingest.silver")


def _j(path: str):
    """get_json_object(body_json, '$.<path>')"""
    return F.get_json_object(F.col("body_json"), f"$.{path}")


class SilverBuilder:
    def __init__(self, spark: SparkSession, cfg: PipelineConfig):
        self.spark = spark
        self.cfg = cfg
        self.t = cfg.target

    def _latest(self, collection: str) -> DataFrame:
        """1 linha por _source_id — a mais recente."""
        src = self.spark.table(self.t.bronze_table(collection))
        w = Window.partitionBy("_source_id").orderBy(
            F.col("_ingestion_timestamp").desc(), F.col("_source_hash").desc()
        )
        return src.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    # ------------------------------------------------------------------ #
    def build_movies(self) -> None:
        base = self._latest("movies")
        movies = base.select(
            F.col("_source_id"),
            _j("title").alias("title"),
            _j("year").cast("int").alias("year"),
            _j("rated").alias("rated"),
            _j("runtime").cast("int").alias("runtime"),
            _j("type").alias("type"),
            _j("plot").alias("plot"),
            F.to_timestamp(_j("released")).alias("released"),
            _j("lastupdated").alias("lastupdated"),
            _j("imdb.rating").cast("double").alias("imdb_rating"),
            _j("imdb.votes").cast("long").alias("imdb_votes"),
            _j("imdb.id").cast("long").alias("imdb_id"),
            _j("tomatoes.viewer.rating").cast("double").alias("tomatoes_viewer_rating"),
            _j("tomatoes.viewer.numReviews").cast("long").alias("tomatoes_viewer_reviews"),
            _j("tomatoes.critic.rating").cast("double").alias("tomatoes_critic_rating"),
            _j("awards.wins").cast("int").alias("awards_wins"),
            _j("awards.nominations").cast("int").alias("awards_nominations"),
            _j("num_mflix_comments").cast("int").alias("num_mflix_comments"),
            F.col("_ingestion_id"),
            F.col("_ingestion_timestamp"),
        )
        self._write(movies, "movies")

        for arr in self.cfg.silver_movies_arrays:
            child = (
                base.select(
                    "_source_id",
                    F.posexplode_outer(
                        F.from_json(_j(arr), "array<string>")
                    ).alias("position", "value"),
                )
                .where(F.col("value").isNotNull())
                .dropDuplicates(["_source_id", "value"])
            )
            self._write(child, f"movies_{arr}")

    def build_comments(self) -> None:
        base = self._latest("comments")
        comments = base.select(
            F.col("_source_id"),
            _j("name").alias("name"),
            _j("email").alias("email"),
            _j("movie_id").alias("movie_id"),
            _j("text").alias("text"),
            F.to_timestamp(_j("date")).alias("date"),
            F.col("_ingestion_id"),
            F.col("_ingestion_timestamp"),
        )
        self._write(comments, "comments")

    def build_users(self) -> None:
        base = self._latest("users")
        users = base.select(
            F.col("_source_id"),
            _j("name").alias("name"),
            _j("email").alias("email"),
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
