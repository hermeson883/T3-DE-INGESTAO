# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %pip install pymongo
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import datetime
import json
 
import bson
from pymongo import MongoClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
 


class MongoReader:

    SCHEMA = StructType([
        StructField("_id", StringType(), True),
        StructField("body", StringType(), True),
    ])

    def __init__(self, database: str = "sample_mflix"):
        self.spark = SparkSession.builder.getOrCreate()
        self.database = database
        self.uri = dbutils.secrets.get(scope="conn-db", key="cnn-mongodb-sampleflix")
        self.client = MongoClient(self.uri, serverSelectionTimeoutMS=15_000,
                                  socketTimeoutMS=300_000, appName="databricks-mongodb-connector")

    def _dbutils(self):
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except Exception:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
 
    @staticmethod
    def _encode(o):
        if isinstance(o, bson.ObjectId):
            return str(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        if isinstance(o, bson.Decimal128):
            return str(o)
        if isinstance(o, bytes):
            return o.hex()
        return str(o)
 
    def collections(self) -> list[str]:
        return sorted(self.client[self.database].list_collection_names())
 
    def count(self, colecao: str, filtro: dict | None = None) -> int:
        return self.client[self.database][colecao].count_documents(filtro or {})
 
    def read(self, colecao: str, filtro: dict | None = None, projecao: dict | None = None,
             limite: int | None = None, batch_size: int = 5_000,
             infer: bool = False, sample_size: int = 1_000) -> DataFrame:
        cursor = self.client[self.database][colecao].find(
            filter=filtro or {}, projection=projecao, batch_size=batch_size
        )
        if limite:
            cursor = cursor.limit(limite)
 
        docs = [(str(d.get("_id", "")), json.dumps(d, default=self._encode, ensure_ascii=False))
                for d in cursor]
        df = self.spark.createDataFrame(docs, schema=self.SCHEMA)
        return self.expand(df, sample_size) if infer else df
 
    def infer_schema(self, df: DataFrame, sample_size: int = 1_000) -> str:
        return df.limit(sample_size).select(F.expr("schema_of_json_agg(body)")).first()[0]
 
    def expand(self, df: DataFrame, sample_size: int = 1_000) -> DataFrame:
        ddl = self.infer_schema(df, sample_size)
        return df.select("_id", F.from_json("body", ddl).alias("doc")).select("_id", "doc.*")
 
    def read_variant(self, colecao: str, **kwargs) -> DataFrame:
        df = self.read(colecao, **kwargs)
        return df.select("_id", F.expr("parse_json(body)").alias("doc"))
 
    def close(self):
        self.client.close()

# COMMAND ----------

df = MongoReader().read(colecao="movies") 
df.display()

# COMMAND ----------

df = MongoReader().read(colecao="movies", infer=True) 
df.display()