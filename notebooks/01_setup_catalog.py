# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup do Unity Catalog (rodar UMA vez por ambiente)
# MAGIC
# MAGIC Cria catalogo, schemas, Volume de landing, diretorios operacionais
# MAGIC (`_checkpoints`, `_schemas`, `_badrecords`) e as tabelas de controle:
# MAGIC `control_ingestion_log`, `ingestion_watermark`, `data_contract_violations`.
# MAGIC Tambem pre-cria as tabelas Bronze com as colunas de rastreabilidade (R4).
# MAGIC
# MAGIC Idempotente: pode rodar de novo sem efeito colateral (`IF NOT EXISTS`).

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo alvo")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from mflix_ingest.config import PipelineConfig
from mflix_ingest.control import ControlManager, bronze_ddl

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH, overrides={"catalog": CATALOG})
t = cfg.target
print("catalogo:", t.catalog)

# COMMAND ----------

# MAGIC %md ## Catalogo, schemas e Volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {t.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t.catalog}.{t.landing_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t.catalog}.{t.bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {t.catalog}.{t.silver_schema}")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {t.catalog}.{t.landing_schema}.{t.landing_volume}"
)
print("volume:", t.volume_root)

# diretorios operacionais dentro do Volume
for coll in cfg.collections:
    for sub in (t.landing_path(coll), t.checkpoint_path(coll),
                t.schema_path(coll), t.badrecords_path(coll)):
        dbutils.fs.mkdirs(sub)
print("diretorios de landing/checkpoints/schemas criados para:", list(cfg.collections))

# COMMAND ----------

# MAGIC %md ## Tabelas de controle (R5)

# COMMAND ----------

ControlManager(spark, t).ensure_tables()

spark.sql(f"COMMENT ON TABLE {t.control_table_fqn} IS "
          f"'R5 — uma linha por execucao por colecao. Fonte de verdade da ingestao.'")
spark.sql(f"COMMENT ON TABLE {t.watermark_table_fqn} IS "
          f"'Watermark persistida por colecao (carga incremental, R3).'")

display(spark.sql(f"SHOW TABLES IN {t.catalog}.{t.bronze_schema}"))

# COMMAND ----------

# MAGIC %md ## Pre-criacao das tabelas Bronze (colunas de rastreabilidade garantidas — R4)

# COMMAND ----------

PART = cfg.bronze.partition_by
for coll in cfg.collections:
    spark.sql(bronze_ddl(t.bronze_table(coll), PART, cfg.bronze.table_properties))
    spark.sql(bronze_ddl(t.bronze_quarantine_table(coll), PART, {}))
    print("bronze:", t.bronze_table(coll))

display(spark.sql(f"DESCRIBE TABLE {t.bronze_table('comments')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pronto
# MAGIC Proximo passo: `notebooks/02_run_pipeline` (ou o Job `jobs/ingestion_job`).