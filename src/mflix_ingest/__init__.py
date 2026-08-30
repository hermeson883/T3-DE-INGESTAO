"""
mflix_ingest — pipeline de ingestao moderna (sample_mflix MongoDB -> Databricks Bronze).

Modulos "puros" (sem pyspark / pymongo no import) — usados tambem nos testes:
    - utils     : run id, timestamps, encoder BSON->JSON, retry com backoff, hashing
    - rules     : construcao de filtro incremental, projection, matematica de reconciliacao
    - config    : dataclasses de configuracao + loader de YAML/JSON
    - contract  : validacao do data contract da origem

Modulos que exigem Spark/PyMongo (so importam quando executados no Databricks):
    - mongo_source : conexao/leitura paginada do MongoDB (pooling + retry)
    - extractor    : MongoDB -> landing (JSON Lines no Volume)
    - loader       : landing -> Bronze via Auto Loader (checkpoint + rescued data)
    - control      : watermark persistida + control_ingestion_log
    - quality      : reconciliacao origem x destino (R8)
    - silver       : camada Silver (movies normalizada) — bonus
    - pipeline     : orquestrador ponta a ponta
"""

__version__ = "1.0.0"
