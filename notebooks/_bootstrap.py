# Databricks notebook source
# MAGIC %md
# MAGIC # _bootstrap — inicializacao comum
# MAGIC Adiciona `src/` ao `sys.path` e resolve os caminhos dos arquivos de config.
# MAGIC **So faz manipulacao de path** — nao instala nada, nao reinicia o Python.
# MAGIC
# MAGIC Cada notebook deve, ANTES de `%run` deste, rodar:
# MAGIC ```
# MAGIC %pip install --quiet pymongo pyyaml
# MAGIC dbutils.library.restartPython()
# MAGIC ```
# MAGIC
# MAGIC Uso:
# MAGIC - de `notebooks/*`  -> `%run ./_bootstrap`
# MAGIC - de `jobs/*`       -> `%run ../notebooks/_bootstrap`

# COMMAND ----------

import os
import sys


def _resolve_repo_root() -> str:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa: F821
        nb = ctx.notebookPath().get()
        logical = "/" + "/".join(nb.strip("/").split("/")[:-2])  # sobe 2 niveis
    except Exception:
        logical = os.getcwd()
    root = logical if logical.startswith("/Workspace") else "/Workspace" + logical
    if not os.path.isdir(root):
        root = logical  # fallback fora de Workspace Files
    return root


REPO_ROOT = _resolve_repo_root()
for _p in (os.path.join(REPO_ROOT, "src"), REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)
try:
    os.chdir(REPO_ROOT)
except Exception as _exc:  # pragma: no cover
    print("aviso: os.chdir falhou:", _exc)

CONFIG_PATH = os.path.join(REPO_ROOT, "config", "pipeline_config.yaml")
COLLECTIONS_PATH = os.path.join(REPO_ROOT, "config", "collections.json")

print("REPO_ROOT        :", REPO_ROOT)
print("CONFIG_PATH      :", CONFIG_PATH)
print("COLLECTIONS_PATH :", COLLECTIONS_PATH)