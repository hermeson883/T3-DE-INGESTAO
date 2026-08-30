# Registro de Contribuições

## Grupo: individual (1 aluno) — Turma 03

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| Hermeson Beserra | 2651199 | Projeto inteiro: `src/mflix_ingest/*` (extract/load/control/quality/silver/contract), `config/*`, `jobs/*`, `notebooks/*`, `tests/*`, `docs/*`, README — R1 a R8 + todos os bônus escolhidos |

## Detalhamento por commit

```bash
git log --oneline --author="Hermeson Beserra"
```

```
6835ec5  criação do projeto
1215e0e  Ajustando o run_pipeline
96e6b35  purge no cache
06f9df8  fix: má escrita
5517d0e  refatorando a extração
114583c  fix: remove VARIANT da Bronze e persist() (nao suportado em Serverless)
4ceaa7e  docs: README sem referencia a VARIANT
aba1816  fix: MERGE com colunas explicitas; aposenta ingest_mode; docs sem VARIANT
c96ea4f  typando os dados para ingestão
3c65c82  Ajustando a silver
```

## Divisão de responsabilidades por requisito

| Item | Responsável | Arquivos |
|---|---|---|
| R1 — pipeline genérica / config | Hermeson Beserra | `config/`, `src/mflix_ingest/config.py`, `pipeline.py` |
| R2 — boas práticas de recurso | Hermeson Beserra | `mongo_source.py`, `extractor.py`, `utils.retry` |
| R3 — incremental + idempotência | Hermeson Beserra | `control.py`, `rules.py`, `loader.py` |
| R4 — rastreabilidade | Hermeson Beserra | `control.bronze_ddl`, `loader._shape` |
| R5 — control_ingestion_log | Hermeson Beserra | `control.ControlManager` |
| R6 — Bronze | Hermeson Beserra | `loader.py`, `01_setup_catalog.py` |
| R7 — schema drift / quarentena | Hermeson Beserra | `loader.py`, `docs/DECISOES_TECNICAS.md` |
| R8 — reconciliação | Hermeson Beserra | `quality.py`, `tests/test_rules.py` |
| Bônus — Silver | Hermeson Beserra | `silver.py`, `jobs/silver_job.py` |
| Bônus — Orquestração | Hermeson Beserra | `notebooks/05_create_workflow.py`, `config/workflow_job.json` |
| Bônus — Observabilidade | Hermeson Beserra | `notebooks/04_dashboard.py`, `config/dashboard_queries.sql` |
| Bônus — Testes | Hermeson Beserra | `tests/` |
| Bônus — Data contract | Hermeson Beserra | `config/data_contract.yaml`, `contract.py` |
| Documentação | Hermeson Beserra | `README.md`, `docs/` |
