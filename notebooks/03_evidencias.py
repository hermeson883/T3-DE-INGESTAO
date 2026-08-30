# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Evidencias das 3 execucoes obrigatorias
# MAGIC
# MAGIC | # | Cenario | Esperado |
# MAGIC |---|---|---|
# MAGIC | 1 | Carga inicial (6 colecoes) | full p/ users/theaters/sessions/embedded_movies; incremental p/ movies/comments; `qtd_lida_origem` bate com os volumes |
# MAGIC | 2 | Incremental **sem novidades** (`comments`) | `qtd_lida_origem = 0`, `status = SUCCESS` |
# MAGIC | 3 | Incremental **com dados novos** (`comments`) | `qtd_lida_origem = N`; Bronze +N; sem duplicidade |
# MAGIC
# MAGIC **Tire um print da saida de cada secao** e salve em `docs/evidencias/`:
# MAGIC `execucao_01_full_load.png`, `execucao_02_incremental_sem_novidades.png`,
# MAGIC `execucao_03_incremental_com_dados.png`.

# COMMAND ----------

# MAGIC %pip install --quiet pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
dbutils.widgets.dropdown("rodar_execucao_1", "false", ["false", "true"], "Rodar full agora?")
CATALOG = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from mflix_ingest.config import PipelineConfig
from mflix_ingest.pipeline import run_pipeline

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH, overrides={"catalog": CATALOG})
CONTROL = cfg.target.control_table_fqn
BRONZE_COMMENTS = cfg.target.bronze_table("comments")


def show_control_for_run(run_id):
    return spark.sql(f"""
        SELECT collection, load_type, watermark_inicial, watermark_final,
               qtd_lida_origem, qtd_gravada_destino, duracao_seg, status,
               divergencia_pct, qtd_duplicada_lote, qtd_quarentena, mensagem_erro
        FROM {CONTROL} WHERE _ingestion_id = '{run_id}'
        ORDER BY collection
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execucao 1 — Carga inicial (todas as 6 colecoes)
# MAGIC Marque o widget `rodar_execucao_1 = true` para executar aqui, ou rode antes
# MAGIC o `02_run_pipeline` / Job `jobs/ingestion_job`.
# MAGIC
# MAGIC Primeira execucao: colecoes `full` -> `load_type=full`; colecoes
# MAGIC `incremental` (movies, comments) extraem tudo (sem watermark ainda) e
# MAGIC **gravam a watermark** ao final. `force_full` NAO e usado aqui — ele e
# MAGIC ferramenta de backfill/reset.

# COMMAND ----------

if dbutils.widgets.get("rodar_execucao_1") == "true":
    s1 = run_pipeline(spark, dbutils, config_path=CONFIG_PATH,
                      collections_path=COLLECTIONS_PATH, collections="all",
                      force_full=False, overrides={"catalog": CATALOG})
    run1 = s1.run_id
else:
    run1 = (spark.table(CONTROL).orderBy("start_time").limit(1)
            .select("_ingestion_id").collect()[0][0])
print("run_id (execucao 1):", run1)

# COMMAND ----------

display(show_control_for_run(run1))

# COMMAND ----------

# MAGIC %md **Conferencia origem x volumes documentados**

# COMMAND ----------

display(spark.sql(f"""
    SELECT collection, load_type, qtd_lida_origem, qtd_gravada_destino, status,
        CASE collection
            WHEN 'movies'          THEN 21000
            WHEN 'comments'        THEN 50000
            WHEN 'users'           THEN   185
            WHEN 'theaters'        THEN  1500
            WHEN 'sessions'        THEN     1
            WHEN 'embedded_movies' THEN  3500
        END AS volume_aprox_documentado
    FROM {CONTROL} WHERE _ingestion_id = '{run1}' ORDER BY collection
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execucao 2 — Incremental SEM novidades (`comments`)
# MAGIC Roda a pipeline so para `comments`. Como a watermark ja esta no maximo,
# MAGIC nada e lido da origem.

# COMMAND ----------

# pre-condicao: a execucao 1 precisa ter gravado a watermark de comments
wm = (spark.table(cfg.target.watermark_table_fqn)
      .where("collection = 'comments'").collect())
assert wm, ("sem watermark para 'comments' — rode a Execucao 1 (full) antes "
            "(widget rodar_execucao_1=true ou notebook 02 com force_full=true).")
print("watermark atual de comments:", wm[0]["watermark_value"])

# COMMAND ----------

s2 = run_pipeline(spark, dbutils, config_path=CONFIG_PATH,
                  collections_path=COLLECTIONS_PATH, collections="comments",
                  overrides={"catalog": CATALOG})
run2 = s2.run_id
display(show_control_for_run(run2))

# COMMAND ----------

r2 = spark.table(CONTROL).where(f"_ingestion_id = '{run2}'").collect()[0]
assert r2["qtd_lida_origem"] == 0, f"esperado 0, veio {r2['qtd_lida_origem']}"
assert r2["status"] == "SUCCESS", f"esperado SUCCESS, veio {r2['status']}"
print("OK — execucao 2: qtd_lida_origem = 0, status = SUCCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execucao 3 — Incremental COM dados novos (`comments`)
# MAGIC Insere 3 documentos de teste na origem com `date` **depois** da watermark atual,
# MAGIC roda a pipeline e valida que so os novos entraram, sem duplicar os antigos.

# COMMAND ----------

import datetime as dt
from bson import ObjectId
from pymongo import MongoClient

N_NOVOS = 3
MARCADOR = "__evidence_test__"

uri = dbutils.secrets.get(scope=cfg.source.secret_scope, key=cfg.source.secret_key)
cli = MongoClient(uri, serverSelectionTimeoutMS=15000)
coll = cli[cfg.source.database]["comments"]

bronze_antes = spark.table(BRONZE_COMMENTS).count()
wm_antes = (spark.table(cfg.target.watermark_table_fqn)
            .where("collection = 'comments'").collect()[0]["watermark_value"])
print("watermark antes:", wm_antes, "| linhas bronze antes:", bronze_antes)

agora = dt.datetime.now(dt.timezone.utc)
novos = [{
    "_id": ObjectId(),
    "name": MARCADOR,
    "email": f"evidencia_{i}@teste.local",
    "movie_id": ObjectId("573a1390f29313caabcd4135"),
    "text": f"comentario de evidencia {i} — {agora.isoformat()}",
    "date": agora + dt.timedelta(seconds=i),
} for i in range(N_NOVOS)]
ins = coll.insert_many(novos)
print(f"{len(ins.inserted_ids)} documentos inseridos na origem")

# COMMAND ----------

s3 = run_pipeline(spark, dbutils, config_path=CONFIG_PATH,
                  collections_path=COLLECTIONS_PATH, collections="comments",
                  overrides={"catalog": CATALOG})
run3 = s3.run_id
display(show_control_for_run(run3))

# COMMAND ----------

bronze_depois = spark.table(BRONZE_COMMENTS).count()
r3 = spark.table(CONTROL).where(f"_ingestion_id = '{run3}'").collect()[0]

dups = spark.sql(f"""
    SELECT _source_id, count(*) n FROM {BRONZE_COMMENTS}
    GROUP BY _source_id HAVING count(*) > 1
""")

print("qtd_lida_origem  :", r3["qtd_lida_origem"], " (esperado", N_NOVOS, ")")
print("linhas bronze    :", bronze_antes, "->", bronze_depois,
      " (delta", bronze_depois - bronze_antes, ")")
print("status           :", r3["status"])
print("_source_id duplicados na bronze:", dups.count())

assert r3["qtd_lida_origem"] == N_NOVOS
assert bronze_depois - bronze_antes == N_NOVOS
assert dups.count() == 0
print("\nOK — execucao 3: apenas os novos entraram, nenhum registro anterior duplicado.")

# COMMAND ----------

display(
    spark.table(BRONZE_COMMENTS)
    .where(f"_ingestion_id = '{run3}'")
    .selectExpr("_source_id",
                "get_json_object(body_json, '$.name')  AS name",
                "get_json_object(body_json, '$.date')  AS date",
                "_ingestion_id", "_ingestion_timestamp", "_load_type", "_ingestion_date")
)

# COMMAND ----------

# MAGIC %md ### Idempotencia — rodar de novo NAO duplica

# COMMAND ----------

s3b = run_pipeline(spark, dbutils, config_path=CONFIG_PATH,
                   collections_path=COLLECTIONS_PATH, collections="comments",
                   overrides={"catalog": CATALOG})
r3b = spark.table(CONTROL).where(f"_ingestion_id = '{s3b.run_id}'").collect()[0]
print("2a rodada seguida -> qtd_lida_origem:", r3b["qtd_lida_origem"],
      "| status:", r3b["status"], "| linhas bronze:", spark.table(BRONZE_COMMENTS).count())
assert r3b["qtd_lida_origem"] == 0

# COMMAND ----------

# MAGIC %md ## Tabela de controle — as 3 execucoes (cole isto no corpo do Pull Request)

# COMMAND ----------

display(spark.sql(f"""
    SELECT _ingestion_id, collection, load_type, watermark_inicial, watermark_final,
           qtd_lida_origem, qtd_gravada_destino, duracao_seg, status, mensagem_erro
    FROM {CONTROL}
    WHERE _ingestion_id IN ('{run1}', '{run2}', '{run3}')
    ORDER BY start_time, collection
"""))

# COMMAND ----------

# MAGIC %md ## Limpeza — remove os documentos de teste da origem

# COMMAND ----------

res = coll.delete_many({"name": MARCADOR})
print(f"{res.deleted_count} documentos de teste removidos da origem.")
cli.close()

# COMMAND ----------

# MAGIC %md
# MAGIC > A Bronze e append-only: os documentos de teste **permanecem** na Bronze
# MAGIC > (fidelidade a origem no instante da ingestao). Para um ambiente limpo,
# MAGIC > recrie a tabela `bronze.comments` via `01_setup_catalog` ou rode com
# MAGIC > outro `catalog`.