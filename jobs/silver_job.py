# Databricks notebook source
# MAGIC %md
# MAGIC # jobs/silver_job — camada Silver (bonus +4)
# MAGIC
# MAGIC Le a Bronze (`body_variant`), deduplica por `_source_id` (latest record) e
# MAGIC normaliza:
# MAGIC - `silver.movies` + `silver.movies_<array>` (explode de cast/genres/directors/...)
# MAGIC - `silver.comments`, `silver.users`
# MAGIC
# MAGIC Sem regra de negocio — apenas achatamento estrutural e tipagem.

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# MAGIC %run ../notebooks/_bootstrap

# COMMAND ----------

from mflix_ingest.config import PipelineConfig
from mflix_ingest.silver import SilverBuilder

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH, overrides={"catalog": CATALOG})

if not cfg.silver_enabled:
    dbutils.notebook.exit("silver.enabled = false — nada a fazer")

built = SilverBuilder(spark, cfg).build_all()
print("Silver construida para:", built)

# COMMAND ----------

for name in ("movies", "movies_cast", "movies_genres", "comments", "users"):
    fqn = cfg.target.silver_table(name)
    if spark.catalog.tableExists(fqn):
        print(f"\n=== {fqn} ({spark.table(fqn).count()} linhas) ===")
        spark.table(fqn).printSchema()

# COMMAND ----------

display(spark.table(cfg.target.silver_table("movies")).limit(20))

# COMMAND ----------

# checagem de dedup — nao pode haver _source_id repetido na Silver
for name in ("movies", "comments", "users"):
    fqn = cfg.target.silver_table(name)
    if spark.catalog.tableExists(fqn):
        dup = (spark.table(fqn).groupBy("_source_id").count()
               .where("count > 1").count())
        assert dup == 0, f"{fqn}: {dup} _source_id duplicado(s)"
        print(f"{fqn}: dedup OK")
