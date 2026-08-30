# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # (LEGADO) Criação de secret
# MAGIC
# MAGIC Este script continha a connection string em texto puro — foi neutralizado.
# MAGIC Use **`notebooks/00_setup_secrets`**, que lê a URI de um widget e nunca a
# MAGIC versiona.
# MAGIC
# MAGIC > O segredo antigo ainda está no HISTÓRICO do git. Antes da entrega:
# MAGIC > `git filter-repo --replace-text ...` + rotacionar a senha na origem.
# MAGIC > Ver seção 7 do README.

# COMMAND ----------

raise SystemExit(
    "Arquivo legado. Rode notebooks/00_setup_secrets (URI via widget)."
)
