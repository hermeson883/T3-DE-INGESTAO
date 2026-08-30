# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Exploracao da origem (sample_mflix)
# MAGIC
# MAGIC EDA para embasar as decisoes de config: volumes, range de watermark,
# MAGIC campos sensiveis/largos, schema drift. Read-only — nao grava nada.

# COMMAND ----------

# MAGIC %pip install --quiet pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from mflix_ingest.config import PipelineConfig
from mflix_ingest.mongo_source import MongoSource

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH)


def uri():
    return dbutils.secrets.get(scope=cfg.source.secret_scope, key=cfg.source.secret_key)

src = MongoSource(cfg.source, uri)

# COMMAND ----------

# MAGIC %md ## Contagem por colecao

# COMMAND ----------

rows = [(c, src.count(c)) for c in src.list_collections()]
display(spark.createDataFrame(rows, "collection string, documentos long"))

# COMMAND ----------

# MAGIC %md ## Range de watermark

# COMMAND ----------

print("comments.date  ->", src.max_value("comments", "date"))
print("movies.lastupdated ->", src.max_value("movies", "lastupdated"))
print("movies sem lastupdated ->",
      src.count("movies", {"lastupdated": {"$exists": False}}))

# COMMAND ----------

# MAGIC %md ## Amostra de cada colecao (com a projection da config aplicada)

# COMMAND ----------

for spec in cfg.resolve_collections("all"):
    docs = src.sample(spec.collection, 2, spec.mongo_projection())
    print(f"\n=== {spec.collection} (projection_exclude={spec.projection_exclude}) ===")
    for d in docs:
        print(sorted(d.keys()))

# COMMAND ----------

# MAGIC %md ## Schema drift — frequencia de chaves em `movies` (amostra 500)

# COMMAND ----------

from collections import Counter

sample = src.sample("movies", 500, cfg.collections["movies"].mongo_projection())
freq = Counter()
for d in sample:
    freq.update(d.keys())
display(spark.createDataFrame(
    sorted(((k, v, round(100 * v / len(sample), 1)) for k, v in freq.items()),
           key=lambda x: -x[1]),
    "campo string, ocorrencias long, pct_docs double",
))

# COMMAND ----------

# MAGIC %md ## Confirma campos sensiveis fora da projection

# COMMAND ----------

for coll, field in [("users", "password"), ("sessions", "jwt"), ("embedded_movies", "plot_embedding")]:
    spec = cfg.collections[coll]
    d = src.sample(coll, 1, spec.mongo_projection())
    present = bool(d) and field in d[0]
    print(f"{coll}.{field} presente na amostra projetada? {present}  (esperado: False)")

# COMMAND ----------

src.close()
print("conexao encerrada")