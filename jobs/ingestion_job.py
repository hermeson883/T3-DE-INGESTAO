# Databricks notebook source
# MAGIC %md
# MAGIC # jobs/ingestion_job — entrypoint da pipeline completa
# MAGIC
# MAGIC MongoDB -> landing -> Bronze (Auto Loader) -> reconciliacao -> watermark -> control_log.
# MAGIC Parametrizado por job parameters / widgets. Sem logica de negocio aqui: apenas
# MAGIC chama `mflix_ingest.pipeline.run_pipeline`.

# COMMAND ----------

# MAGIC %pip install --quiet pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
dbutils.widgets.text("collections", "all", "Colecoes (all | comments,users ...)")
dbutils.widgets.dropdown("force_full", "false", ["false", "true"], "force_full")
dbutils.widgets.dropdown("engine", "batch", ["batch", "autoloader"], "Motor de carga")

CATALOG = dbutils.widgets.get("catalog").strip()
COLLECTIONS = dbutils.widgets.get("collections").strip()
FORCE_FULL = dbutils.widgets.get("force_full").strip().lower() == "true"
ENGINE = dbutils.widgets.get("engine").strip()

# COMMAND ----------

# MAGIC %run ../notebooks/_bootstrap

# COMMAND ----------

from mflix_ingest.pipeline import run_pipeline, SUMMARY_SCHEMA

summary = run_pipeline(
    spark,
    dbutils,
    config_path=CONFIG_PATH,
    collections_path=COLLECTIONS_PATH,
    collections=COLLECTIONS,
    force_full=FORCE_FULL,
    overrides={"catalog": CATALOG, "engine": ENGINE},
)

rows = summary.to_rows()
for r in rows:
    print(f"  {r['collection']:<16} {r['status']:<8} "
          f"lida={r['qtd_lida_origem']:<8} gravada={r['qtd_gravada_destino']:<8} "
          f"div={r['divergencia_pct']}%  {r['duracao_seg']}s")

# COMMAND ----------

display(spark.createDataFrame(rows, SUMMARY_SCHEMA))

# COMMAND ----------

# expõe o run_id para tasks seguintes do Workflow
try:
    dbutils.jobs.taskValues.set(key="run_id", value=summary.run_id)
    dbutils.jobs.taskValues.set(key="catalog", value=CATALOG)
except Exception:
    pass

# COMMAND ----------

falhas = [r["collection"] for r in rows if r["status"] == "FAILED"]
if falhas:
    raise Exception(f"ingestion_job: colecoes em FAILED -> {falhas}")
print("ingestion_job OK — run_id:", summary.run_id)