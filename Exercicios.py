# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ./mongo_reader

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import * 

df = MongoReader().read(colecao="movies", infer=True) 

# Revelar tipos inferidos pelo Spark
df.printSchema()

# Análise exploratória dos 4 campos
df.select(
    "year",
    col("imdb.rating").alias("imdb_rating"),
    col("imdb.votes").alias("imdb_votes"),
    "runtime","languages"
).show(10)

# Tabela de análise de tipos
# Campo          | Tipo Inferido        | Motivo
# year           | StringType           | Valores mistos: "2010", "TV Movie, 2015"
# imdb.rating    | DoubleType/String    | Nulos representados como ""
# imdb.votes     | IntegerType/String   | Varia por amostragem
# runtime        | IntegerType          | Pode conter nulos

# Confirmar via dtypes
df.dtypes

# COMMAND ----------


# Seleção de campos relevantes com aliases
df_sel = df.select(
    "year",
    col("imdb.rating").alias("imdb_rating"),
    col("imdb.votes").alias("imdb_votes"),
    "runtime", "languages"
)

# Exibição do schema resultante
df_sel.printSchema()

# Validação: distribuição e nulos
df_sel.describe().show()

# Pontos de Validação:
# - describe().show() — checar contagem, média, min e max de cada campo
# - Verificar se imdb_rating foi inferido como double ou string — depende da amostra
# - df_sel.filter(col("year").isNull()).count() — checar volume de nulos

# COMMAND ----------


from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

schema_movies = StructType([
    StructField("year", StringType(), nullable=True),
    StructField("imdb_rating", DoubleType(), nullable=True),
    StructField("imdb_votes", IntegerType(), nullable=True),
    StructField("runtime", IntegerType(), nullable=True),
    StructField("languages", StringType(), nullable=True)
])

df_typed = df_sel.withColumn("year", col("year").cast(StringType()))
df_typed.display()

# COMMAND ----------

from pyspark.sql.functions import when, lit, current_timestamp, col, size

df_rastr = (
    df_typed.withColumn("origem", lit("mongodb://samples_mflix.movies"))
        .withColumn(
            "quarentena_em",
            when(
                col("languages").isNull() | (size(col("languages")) == 0),
                current_timestamp(),
            ).otherwise(lit(None).cast("timestamp")),
        )
        .withColumn(
            "motivo",
            when(
                col("languages").isNull() | (size(col("languages")) == 0),
                lit("languages não informado"),
            ).otherwise(lit(None)),
        )
)
df_rastr.display()

# COMMAND ----------

df_rastr.write.mode("overwrite").saveAsTable("meu_catalog.bronze.tb_movies")