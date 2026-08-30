# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup de Secrets (rodar UMA vez)
# MAGIC
# MAGIC Cria o *secret scope* `conn-db` e grava a **connection string do MongoDB**.
# MAGIC
# MAGIC > **A URI vem de um widget — NUNCA e escrita no codigo nem versionada.**
# MAGIC > Cole a URI no widget `mongo_uri`, execute e depois use
# MAGIC > `Edit > Clear > Clear All Cell Outputs` antes de commitar/exportar.
# MAGIC
# MAGIC O enunciado permite (e recomenda) separar a criacao de secrets num notebook
# MAGIC proprio devido ao ambiente limitado.

# COMMAND ----------

# MAGIC %pip install --quiet pymongo
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("secret_scope", "conn-db", "1. Secret scope")
dbutils.widgets.text("secret_key", "cnn-mongodb-sampleflix", "2. Secret key")
dbutils.widgets.text("mongo_uri", "", "3. MongoDB URI (mongodb://user:pass@host:porta/...)")

SCOPE = dbutils.widgets.get("secret_scope").strip()
KEY = dbutils.widgets.get("secret_key").strip()
URI = dbutils.widgets.get("mongo_uri").strip()

assert SCOPE and KEY, "informe scope e key"
assert URI.startswith("mongodb"), "cole a URI no widget 'mongo_uri' (mongodb:// ou mongodb+srv://)"

# COMMAND ----------

import requests

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
instance = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}

# cria o scope (400 = ja existe, tudo bem)
r = requests.post(
    f"{instance}/api/2.0/secrets/scopes/create",
    headers=headers,
    json={"scope": SCOPE, "initial_manage_principal": "users"},
)
print("scope:", "ok" if r.status_code in (200, 400) else f"ERRO {r.status_code} {r.text}")

# grava a secret
r = requests.post(
    f"{instance}/api/2.0/secrets/put",
    headers=headers,
    json={"scope": SCOPE, "key": KEY, "string_value": URI},
)
assert r.status_code == 200, f"ERRO ao gravar secret: {r.status_code} {r.text}"
print(f"secret {SCOPE}/{KEY} gravada com sucesso.")

# COMMAND ----------

# MAGIC %md ## Verificacao (nao imprime a URI inteira)

# COMMAND ----------

val = dbutils.secrets.get(scope=SCOPE, key=KEY)
print("comprimento:", len(val), "| prefixo:", val[:10] + "...")

# COMMAND ----------

# MAGIC %md ## Teste de conexao com o MongoDB

# COMMAND ----------

from pymongo import MongoClient

cli = MongoClient(dbutils.secrets.get(scope=SCOPE, key=KEY), serverSelectionTimeoutMS=15000)
print("ping:", cli.admin.command("ping"))
db = cli["sample_mflix"]
print("colecoes:", sorted(db.list_collection_names()))
for c in ("movies", "comments", "users", "theaters", "sessions", "embedded_movies"):
    try:
        print(f"  {c:<16} {db[c].estimated_document_count():>8}")
    except Exception as e:
        print(f"  {c:<16} (erro: {e})")
cli.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpe as saidas antes de sair
# MAGIC `Edit > Clear > Clear All Cell Outputs` — para nao deixar o resultado do
# MAGIC `ping` / lista de colecoes exposto no arquivo versionado.
