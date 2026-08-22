# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import requests

# Fetch current workspace URL and authentication token automatically
instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}
payload = {
    "scope": "conn-db",
    "initial_manage_principal": "users"  # Options: "users" or "creator"
}

try: 
    response = requests.post(f"{instance}/api/2.0/secrets/scopes/create", headers=headers, json=payload)
except Exception as e:
    pass #print(f"Error: {e}")    

if response.status_code in (200, 400):
    print("Secret scope 'conn-db' created successfully.")

# COMMAND ----------

import requests

instance = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}"}
payload = {
    "scope": "conn-db",
    "key": "cnn-mongodb-sampleflix",
    "string_value": "REDACTED"
}

response = requests.post(f"{instance}/api/2.0/secrets/put", headers=headers, json=payload)

if response.status_code == 200:
    print("Secret criada/atualizada com sucesso!")
else:
    print(f"Erro ({response.status_code}): {response.text}")