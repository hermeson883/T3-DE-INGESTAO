# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Executar a pipeline (interativo)
# MAGIC
# MAGIC Orquestrador ponta a ponta: **extract (Mongo -> landing) -> load (Auto Loader -> Bronze)
# MAGIC -> reconciliacao -> watermark -> control_ingestion_log**.
# MAGIC
# MAGIC Mesma logica do Job `jobs/ingestion_job` — este notebook so adiciona visualizacoes.
# MAGIC
# MAGIC | widget | efeito |
# MAGIC |---|---|
# MAGIC | `collections` | `all` ou lista: `comments,users` |
# MAGIC | `catalog` | sobrescreve o catalogo alvo |
# MAGIC | `force_full` | `true` -> ignora watermark e recarrega tudo como full |

# COMMAND ----------

# MAGIC %pip install --quiet pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("collections", "all", "Colecoes")
dbutils.widgets.text("catalog", "mflix", "Catalogo")
dbutils.widgets.dropdown("force_full", "false", ["false", "true"], "force_full")
dbutils.widgets.dropdown("engine", "batch", ["batch", "autoloader"], "Motor de carga")

COLLECTIONS = dbutils.widgets.get("collections").strip()
CATALOG = dbutils.widgets.get("catalog").strip()
FORCE_FULL = dbutils.widgets.get("force_full") == "true"
ENGINE = dbutils.widgets.get("engine").strip()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from mflix_ingest.pipeline import run_pipeline

summary = run_pipeline(
    spark,
    dbutils,
    config_path=CONFIG_PATH,
    collections_path=COLLECTIONS_PATH,
    collections=COLLECTIONS,
    force_full=FORCE_FULL,
    overrides={"catalog": CATALOG, "engine": ENGINE},
)

print("run_id:", summary.run_id, "| ok:", summary.ok)

# COMMAND ----------

# MAGIC %md ## Resumo desta execucao

# COMMAND ----------

display(spark.createDataFrame(summary.to_rows()))

# COMMAND ----------

# MAGIC %md ## control_ingestion_log — historico completo

# COMMAND ----------

display(
    spark.table(f"{CATALOG}.bronze.control_ingestion_log")
    .orderBy("start_time", "collection")
)

# COMMAND ----------

# MAGIC %md ## Watermarks atuais

# COMMAND ----------

display(spark.table(f"{CATALOG}.bronze.ingestion_watermark").orderBy("collection"))

# COMMAND ----------

# MAGIC %md ## Amostra da Bronze (documento preservado como veio — R6)

# COMMAND ----------

tbl = (COLLECTIONS.split(",")[0].strip() if COLLECTIONS not in ("all", "") else "comments")
display(
    spark.table(f"{CATALOG}.bronze.{tbl}")
    .select("_source_id", "body_json", "_rescued_data", "_ingestion_id",
            "_ingestion_timestamp", "_source_path", "_load_type", "_ingestion_date")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Falha o notebook se alguma colecao terminou em `FAILED`
# MAGIC (util quando executado como task de Job).

# COMMAND ----------

falhas = [r.collection for r in summary.records if r.status == "FAILED"]
if falhas:
    raise Exception(f"Execucao com falha nas colecoes: {falhas}")
print("Execucao OK.")