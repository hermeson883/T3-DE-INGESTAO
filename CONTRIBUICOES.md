# Registro de Contribuições

## Grupo: `<preencher — nome do grupo>` — Turma `<XX>`

> **[Importante]** Este arquivo é avaliado. Preencha nome, matrícula e
> contribuições de cada membro. Membros sem contribuição identificável no
> histórico de commits **e** sem descrição aqui perdem pontos (rubrica).

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| `<Nome 1>` | `<XXXXXX>` | `<ex.: mongo_source.py / extractor.py, projection e pushdown, notebook 06 (EDA), config/collections.json>` |
| `<Nome 2>` | `<XXXXXX>` | `<ex.: loader.py (Auto Loader), estratégia de idempotência (MERGE/append), docs/ARQUITETURA.md>` |
| `<Nome 3>` | `<XXXXXX>` | `<ex.: control.py / quality.py, watermark, reconciliação (R8), notebook 03 (evidências)>` |

## Detalhamento por commit

Cole a saída de, para cada membro:

```bash
git log --oneline --author="<Nome>"
```

```
<Nome 1>
<hash>  <mensagem>
...

<Nome 2>
...

<Nome 3>
...
```

## Divisão de responsabilidades por requisito (sugestão de preenchimento)

| Item | Responsável | Arquivos |
|---|---|---|
| R1 — pipeline genérica / config | | `config/`, `src/mflix_ingest/config.py`, `pipeline.py` |
| R2 — boas práticas de recurso | | `mongo_source.py`, `extractor.py`, `utils.retry` |
| R3 — incremental + idempotência | | `control.py`, `rules.py`, `loader.py` |
| R4 — rastreabilidade | | `control.bronze_ddl`, `loader._enrich` |
| R5 — control_ingestion_log | | `control.ControlManager` |
| R6 — Bronze | | `loader.py`, `01_setup_catalog.py` |
| R7 — schema drift / quarentena | | `loader.py`, `docs/DECISOES_TECNICAS.md` |
| R8 — reconciliação | | `quality.py`, `tests/test_rules.py` |
| Bônus — Silver | | `silver.py`, `jobs/silver_job.py` |
| Bônus — Orquestração | | `notebooks/05_create_workflow.py` |
| Bônus — Observabilidade | | `notebooks/04_dashboard.py`, `config/dashboard_queries.sql` |
| Bônus — Testes | | `tests/` |
| Bônus — Data contract | | `config/data_contract.yaml`, `contract.py` |
| Documentação | | `README.md`, `docs/` |
