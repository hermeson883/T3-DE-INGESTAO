# Databricks notebook source
# MAGIC %md
# MAGIC # jobs/bronze_job — consumidor independente da landing zone
# MAGIC
# MAGIC **Nao acessa o MongoDB.** Le o que ja existe na landing (`/Volumes/<cat>/landing/mflix_raw/<coll>/`)
# MAGIC via Auto Loader e materializa na Bronze, com reconciliacao e `control_ingestion_log`.
# MAGIC
# MAGIC Usos:
# MAGIC - **backfill / reprocessamento** sem re-extrair da origem (o checkpoint garante
# MAGIC   que arquivos ja processados nao voltam);
# MAGIC - **ingestao orientada a arquivos pura** — arquivos podem ser depositados por
# MAGIC   qualquer processo, nao so pelo `ingestion_job`.
# MAGIC
# MAGIC Nao ha Mongo para contar aqui: `qtd_lida_origem` = linhas que o Auto Loader
# MAGIC entregou nesta passada (arquivos novos segundo o checkpoint). A reconciliacao
# MAGIC foca em chave nula / duplicidade no lote. Para a divergencia origem x destino
# MAGIC "de verdade", use `jobs/ingestion_job`.

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
dbutils.widgets.text("collections", "all", "Colecoes")
dbutils.widgets.dropdown("engine", "autoloader", ["autoloader", "batch"], "Motor de carga")

CATALOG = dbutils.widgets.get("catalog").strip()
COLLECTIONS = dbutils.widgets.get("collections").strip()
ENGINE = dbutils.widgets.get("engine").strip()

# COMMAND ----------

# MAGIC %run ../notebooks/_bootstrap

# COMMAND ----------

from mflix_ingest import __version__
from mflix_ingest.config import PipelineConfig
from mflix_ingest.control import ControlManager, ControlRecord
from mflix_ingest.loader import BronzeLoader
from mflix_ingest.quality import Reconciler
from mflix_ingest.utils import new_run_id, utc_now

# bronze_job = consumidor independente da landing -> autoloader por padrao (bonus +5)
cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH,
                          overrides={"catalog": CATALOG, "engine": ENGINE})
run_id = new_run_id()
ts = utc_now()

control = ControlManager(spark, cfg.target)
control.ensure_tables()
reconciler = Reconciler(spark, cfg, control)
loader = BronzeLoader(spark, cfg.target, cfg.autoloader, cfg.bronze)

# COMMAND ----------

results = []
for spec in cfg.resolve_collections(COLLECTIONS):
    rec = ControlRecord(ingestion_id=run_id, collection=spec.collection,
                        load_type=spec.load_mode,
                        ingest_mode=cfg.autoloader.engine,
                        pipeline_version=__version__)
    try:
        ld = loader.load(spec, run_id, ts, cfg.source.source_path_tag)
        # sem origem independente: "lida" = o que o Auto Loader entregou nesta passada
        rep = reconciler.evaluate(spec, run_id, ld.rows_written, ld.rows_written)
        rec.finish(rep.status, qtd_lida_origem=ld.rows_written,
                   qtd_gravada_destino=ld.rows_written, qtd_quarentena=ld.rows_quarantined,
                   qtd_duplicada_lote=rep.batch_duplicates,
                   divergencia_pct=rep.outcome.divergence_pct,
                   mensagem_erro=None if rep.status == "SUCCESS" else rep.message)
    except Exception as exc:  # noqa: BLE001
        rec.finish("FAILED", mensagem_erro=f"{type(exc).__name__}: {exc}")
    control.log_run(rec)
    results.append(rec)

# COMMAND ----------

display(
    spark.table(cfg.target.control_table_fqn)
    .where(f"_ingestion_id = '{run_id}'").orderBy("collection")
)

# COMMAND ----------

falhas = [r.collection for r in results if r.status == "FAILED"]
if falhas:
    raise Exception(f"bronze_job: FAILED -> {falhas}")
print("bronze_job OK — run_id:", run_id)