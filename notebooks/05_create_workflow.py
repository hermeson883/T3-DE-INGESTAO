# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Orquestracao: cria/atualiza o Job (bonus +4)
# MAGIC
# MAGIC Workflow com dependencias, retry e notificacao de falha:
# MAGIC
# MAGIC ```
# MAGIC setup_catalog ─▶ ingestao (retry 2x) ─▶ silver ─▶ reconciliacao
# MAGIC ```
# MAGIC
# MAGIC - `setup_catalog`  -> notebooks/01_setup_catalog
# MAGIC - `ingestao`       -> jobs/ingestion_job         (max_retries=2, retry_on_timeout)
# MAGIC - `silver`         -> jobs/silver_job
# MAGIC - `reconciliacao`  -> jobs/bronze_job (modo audit) / 04_dashboard checagem
# MAGIC - agendamento diario **PAUSADO** + `email_notifications.on_failure`
# MAGIC
# MAGIC Tambem grava a definicao em `config/workflow_job.json` (versionavel, sem segredo).

# COMMAND ----------

# MAGIC %pip install --quiet pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "mflix", "Catalogo")
dbutils.widgets.text("notification_email", "", "E-mail p/ falha (opcional)")
dbutils.widgets.text("schedule_cron", "0 0 6 * * ?", "Cron (Quartz) — agendamento")
CATALOG = dbutils.widgets.get("catalog").strip()
EMAIL = dbutils.widgets.get("notification_email").strip()
CRON = dbutils.widgets.get("schedule_cron").strip()

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import json
import os

from mflix_ingest.config import PipelineConfig

cfg = PipelineConfig.load(CONFIG_PATH, COLLECTIONS_PATH, overrides={"catalog": CATALOG})
orc = cfg.orchestration

# caminho LOGICO do repo no workspace (sem prefixo /Workspace)
repo_logical = REPO_ROOT[len("/Workspace"):] if REPO_ROOT.startswith("/Workspace") else REPO_ROOT
def nb(rel: str) -> str:
    return f"{repo_logical}/{rel}"

emails = [EMAIL] if EMAIL else list(orc.on_failure_emails)

job_settings = {
    "name": orc.job_name,
    "max_concurrent_runs": 1,
    "timeout_seconds": orc.timeout_seconds,
    "tags": {"projeto": "t3-de-ingestao", "camada": "bronze"},
    "parameters": [
        {"name": "catalog", "default": CATALOG},
        {"name": "collections", "default": "all"},
        {"name": "force_full", "default": "false"},
    ],
    "job_clusters": [],
    "tasks": [
        {
            "task_key": "setup_catalog",
            "notebook_task": {
                "notebook_path": nb("notebooks/01_setup_catalog"),
                "base_parameters": {"catalog": "{{job.parameters.catalog}}"},
            },
        },
        {
            "task_key": "ingestao",
            "depends_on": [{"task_key": "setup_catalog"}],
            "notebook_task": {
                "notebook_path": nb("jobs/ingestion_job"),
                "base_parameters": {
                    "catalog": "{{job.parameters.catalog}}",
                    "collections": "{{job.parameters.collections}}",
                    "force_full": "{{job.parameters.force_full}}",
                },
            },
            "max_retries": orc.max_retries,
            "min_retry_interval_millis": orc.min_retry_interval_millis,
            "retry_on_timeout": orc.retry_on_timeout,
        },
        {
            "task_key": "silver",
            "depends_on": [{"task_key": "ingestao"}],
            "notebook_task": {
                "notebook_path": nb("jobs/silver_job"),
                "base_parameters": {"catalog": "{{job.parameters.catalog}}"},
            },
            "max_retries": 1,
        },
        {
            "task_key": "reconciliacao",
            "depends_on": [{"task_key": "silver"}],
            "notebook_task": {
                "notebook_path": nb("notebooks/04_dashboard"),
                "base_parameters": {"catalog": "{{job.parameters.catalog}}"},
            },
        },
    ],
    "schedule": {
        "quartz_cron_expression": CRON,
        "timezone_id": "America/Sao_Paulo",
        "pause_status": "PAUSED",
    },
}
if emails:
    job_settings["email_notifications"] = {"on_failure": emails, "no_alert_for_skipped_runs": True}
    for task in job_settings["tasks"]:
        if task["task_key"] == "ingestao":
            task["email_notifications"] = {"on_failure": emails}

# grava a definicao versionavel (sem segredo)
out = os.path.join(REPO_ROOT, "config", "workflow_job.json")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(job_settings, fh, indent=2, ensure_ascii=False)
print("definicao salva em:", out)
print(json.dumps(job_settings, indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md ## Cria ou atualiza o Job (REST API — sem dependencia de versao de SDK)

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}

lst = requests.get(f"{host}/api/2.2/jobs/list", headers=headers,
                   params={"name": orc.job_name}).json().get("jobs", [])
if lst:
    job_id = lst[0]["job_id"]
    r = requests.post(f"{host}/api/2.2/jobs/reset", headers=headers,
                      json={"job_id": job_id, "new_settings": job_settings})
    assert r.status_code == 200, r.text
    print("Job atualizado:", job_id)
else:
    r = requests.post(f"{host}/api/2.2/jobs/create", headers=headers, json=job_settings)
    assert r.status_code == 200, r.text
    job_id = r.json()["job_id"]
    print("Job criado:", job_id)

ws_url = spark.conf.get("spark.databricks.workspaceUrl", None)
print(f"URL: https://{ws_url}/jobs/{job_id}" if ws_url else f"job_id={job_id}")

# COMMAND ----------

# MAGIC %md ## Disparar uma execucao agora (opcional)

# COMMAND ----------

dbutils.widgets.dropdown("disparar_agora", "false", ["false", "true"], "Disparar agora?")
if dbutils.widgets.get("disparar_agora") == "true":
    r = requests.post(f"{host}/api/2.2/jobs/run-now", headers=headers,
                      json={"job_id": job_id, "job_parameters": {"force_full": "true"}})
    print("run:", r.json(), "-> acompanhe na aba Runs do Job")
else:
    print("marque 'disparar_agora=true' para executar")