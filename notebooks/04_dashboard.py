# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Observabilidade (bonus +3)
# MAGIC
# MAGIC Consultas sobre `control_ingestion_log`. Rode as celulas para conferir e,
# MAGIC para o dashboard de verdade:
# MAGIC
# MAGIC 1. **SQL Editor** -> cole cada query de `config/dashboard_queries.sql`
# MAGIC 2. **Salvar** cada uma como *Query*
# MAGIC 3. **Dashboards -> Create dashboard** -> adicione uma visualizacao por query:
# MAGIC    - Volume/dia (barras empilhadas: `gravados_bronze` por `collection`)
# MAGIC    - Duracao (linha: `duracao_seg` no tempo)
# MAGIC    - Taxa de falha (linha: `pct_falha` por dia)
# MAGIC    - Ultimo status por colecao (tabela / counters)
# MAGIC    - Progressao da watermark (tabela)
# MAGIC 4. Print do dashboard -> `docs/evidencias/observabilidade.png`

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

# MAGIC %md
# MAGIC As mesmas consultas estao em `config/dashboard_queries.sql` (com `{{catalog}}`)
# MAGIC prontas para colar no SQL Editor / Dashboard.

# COMMAND ----------

# MAGIC %md ## 1) Volume ingerido por dia e colecao

# COMMAND ----------

display(spark.sql(f"""
  SELECT CAST(start_time AS DATE) AS dia, collection,
         SUM(qtd_lida_origem) AS lidos_origem,
         SUM(qtd_gravada_destino) AS gravados_bronze,
         SUM(qtd_quarentena) AS quarentena
  FROM {CATALOG}.bronze.control_ingestion_log
  GROUP BY 1,2 ORDER BY 1 DESC, 2
"""))

# COMMAND ----------

# MAGIC %md ## 2) Duracao e throughput por execucao

# COMMAND ----------

display(spark.sql(f"""
  SELECT start_time, collection, duracao_seg, qtd_gravada_destino,
         ROUND(qtd_gravada_destino / NULLIF(duracao_seg,0), 1) AS linhas_por_seg
  FROM {CATALOG}.bronze.control_ingestion_log
  ORDER BY start_time DESC
"""))

# COMMAND ----------

# MAGIC %md ## 3) Taxa de falha / partial por dia

# COMMAND ----------

display(spark.sql(f"""
  SELECT CAST(start_time AS DATE) AS dia, COUNT(*) AS execucoes,
         SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
         SUM(CASE WHEN status='PARTIAL' THEN 1 ELSE 0 END) AS partial,
         SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS failed,
         ROUND(100.0*SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_falha
  FROM {CATALOG}.bronze.control_ingestion_log
  GROUP BY 1 ORDER BY 1 DESC
"""))

# COMMAND ----------

# MAGIC %md ## 4) Ultimo status por colecao

# COMMAND ----------

display(spark.sql(f"""
  WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY collection ORDER BY start_time DESC) rn
    FROM {CATALOG}.bronze.control_ingestion_log)
  SELECT collection, load_type, status, qtd_lida_origem, qtd_gravada_destino,
         divergencia_pct, duracao_seg, end_time
  FROM ranked WHERE rn = 1 ORDER BY collection
"""))

# COMMAND ----------

# MAGIC %md ## 5) Progressao da watermark

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.bronze.ingestion_watermark ORDER BY collection"))

# COMMAND ----------

# MAGIC %md ## 6) Reconciliacao acumulada (control_log x Bronze real)

# COMMAND ----------

from mflix_ingest.config import PipelineConfig

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH, overrides={"catalog": CATALOG})
rows = []
for coll in cfg.collections:
    tbl = cfg.target.bronze_table(coll)
    if not spark.catalog.tableExists(tbl):
        continue
    acc = spark.sql(f"""SELECT SUM(qtd_gravada_destino) s FROM {CATALOG}.bronze.control_ingestion_log
                        WHERE collection = '{coll}' AND status <> 'FAILED'""").collect()[0]["s"] or 0
    real = spark.table(tbl).count()
    rows.append((coll, int(acc), int(real), int(real - acc)))
display(spark.createDataFrame(rows, "collection string, soma_control_log long, linhas_bronze long, delta long"))