# Ingestão Moderna de Dados — `sample_mflix` → Bronze (Databricks)

Pipeline de ingestão genérica e parametrizada que extrai as 6 coleções do
MongoDB `sample_mflix` e materializa uma **camada Bronze (Delta)** no Unity
Catalog, com **rastreabilidade completa** de cada registro, carga incremental
com watermark persistida, idempotência, reconciliação e observabilidade.

> Trabalho Final — Engenharia de Dados / Ingestão Moderna de Dados.
> Enunciado em [`trabalho_final_ingestao_moderna.md`](trabalho_final_ingestao_moderna.md),
> entrega em [`SEND_WORK.md`](SEND_WORK.md).

---

## 1. Arquitetura (visão rápida)

```
MongoDB sample_mflix
   │  count + find(filter incremental, projection/pushdown, batchSize)  ── retry/backoff, 1 conexão (pool)
   ▼
landing.mflix_raw  (Volume)   <collection>/<collection>_<run_id>_<ts>.jsonl   ── cópia byte-a-byte da origem (R6)
   ▼  motor de carga (config autoloader.engine):
   │    batch (padrão) ── spark.read dos arquivos DA execução · rápido, sem streaming
   │    autoloader     ── readStream cloudFiles · checkpoint · schemaLocation persistido  (bônus +5, via bronze_job)
bronze.<collection>  (Delta)   append (incremental) | MERGE _source_id (full) · partição _ingestion_date
   ├── bronze.<collection>_quarentena        registros inválidos (R7)
   ├── bronze.control_ingestion_log          1 linha por execução por coleção (R5)
   ├── bronze.ingestion_watermark            watermark persistida (R3)
   └── bronze.data_contract_violations       data contract (bônus)
   ▼
silver.movies / movies_cast / movies_genres / comments / users   dedup _source_id + normalização (bônus +4)
```

Diagrama Mermaid completo e nomenclatura: **[`docs/ARQUITETURA.md`](docs/ARQUITETURA.md)**.
Justificativas e trade-offs: **[`docs/DECISOES_TECNICAS.md`](docs/DECISOES_TECNICAS.md)**.

---

## 2. Estrutura do repositório

```
config/
  pipeline_config.yaml       configuração global (SEM credenciais)
  collections.json           parâmetros por coleção (modo_carga, watermark, projection...)
  data_contract.yaml         contrato formal da origem (bônus)
  dashboard_queries.sql      queries da observabilidade
  workflow_job.json          definição do Job (gerada pelo notebook 05)
src/mflix_ingest/
  utils.py  rules.py  config.py  contract.py     ← puros (cobertos por testes)
  mongo_source.py     extract (cursor paginado, pooling, retry)
  extractor.py        MongoDB → landing (JSON Lines)
  loader.py           landing → Bronze (motor batch [padrão] ou autoloader; MERGE/append)
  control.py          watermark + control_ingestion_log + DDL
  quality.py          reconciliação origem × destino (R8)
  silver.py           camada Silver (bônus)
  pipeline.py         orquestrador ponta a ponta
jobs/
  ingestion_job.py    entrypoint da pipeline completa
  bronze_job.py       consumidor independente da landing (backfill / file-driven)
  silver_job.py       build da Silver
notebooks/
  _bootstrap.py           setup de path (usado via %run)
  00_setup_secrets.py     cria o secret scope a partir de um widget (rodar 1x)
  01_setup_catalog.py     cria catálogo/schemas/Volume/tabelas (rodar 1x)
  02_run_pipeline.py      executa a pipeline (interativo, com visualizações)
  03_evidencias.py        gera as 3 evidências obrigatórias
  04_dashboard.py         observabilidade
  05_create_workflow.py   cria/atualiza o Job (bônus orquestração)
  06_explore_source.py    EDA da origem
tests/                     pytest (puro Python, sem Spark)
docs/
  ARQUITETURA.md  DECISOES_TECNICAS.md  SAMPLE_MFLIX.md  evidencias/
CONTRIBUICOES.md
```

---

## 3. Como executar

### Pré-requisitos
- Workspace Databricks com **Unity Catalog** e permissão de `CREATE CATALOG`
  (ou um catálogo já existente — passe o nome no widget `catalog`).
- **Serverless** ou **DBR 14.3 LTS+** (Delta + Unity Catalog; nenhum tipo exótico —
  a Bronze usa só `STRING`/`TIMESTAMP`/`DATE`).
- Acesso de rede ao MongoDB de origem.

### Passo a passo

1. **Suba o repositório** e clone como **Git Folder** no Databricks
   (`Workspace → Repos → Add Repo`).

2. **`notebooks/00_setup_secrets`** — no widget `mongo_uri`, cole a connection
   string do MongoDB. Execute. Limpe as saídas depois.
   Cria o secret `conn-db / cnn-mongodb-sampleflix`.

3. **`notebooks/01_setup_catalog`** — widget `catalog` (default `mflix`).
   Cria catálogo, schemas `landing`/`bronze`/`silver`, o Volume, os diretórios
   operacionais e todas as tabelas de controle + Bronze.

4. **`notebooks/02_run_pipeline`** — `collections=all`, `force_full=false`.
   Ou rode o Job `jobs/ingestion_job`.

5. **`notebooks/03_evidencias`** — gera as 3 execuções obrigatórias e a query
   final para o PR. Salve os prints em `docs/evidencias/`.

6. (bônus) **`jobs/silver_job`**, **`notebooks/04_dashboard`**,
   **`notebooks/05_create_workflow`**.

### Parâmetros (widgets / job parameters)

| Parâmetro | Default | Efeito |
|---|---|---|
| `catalog` | `mflix` | catálogo alvo (override de `pipeline_config.yaml`) |
| `collections` | `all` | `all` ou lista: `comments,users` |
| `force_full` | `false` | ignora watermark, recarrega tudo como `full` |
| `engine` | `batch` | `batch` (rápido, sem streaming) ou `autoloader` (readStream + checkpoint) |

Toda a demais configuração está em `config/` — **nada hardcoded** (R1).

---

## 4. Requisitos obrigatórios — onde cada um está

| Req | Resumo | Implementação |
|---|---|---|
| **R1** | Pipeline genérica e parametrizada, OOP, config externalizada | `config/*` + `src/mflix_ingest/config.py`; extract/load/control separados em módulos; `pipeline.run_pipeline` roda as 6 coleções com o mesmo código |
| **R2** | ≥4 boas práticas de recurso, justificadas | cursor em lotes, projection/pushdown, sem `collect/toPandas/list`, partição no destino, pooling, retry+backoff — tabela em [ARQUITETURA §5](docs/ARQUITETURA.md#5-boas-práticas-de-recurso-r2--onde-estão-no-código) |
| **R3** | Full + incremental com watermark persistida + idempotência | `full` (users/theaters/sessions/embedded_movies) via MERGE por `_source_id`; `incremental` (comments/movies) via append + `bronze.ingestion_watermark`. O motor `batch` lê só o arquivo `.jsonl` **daquela execução** (`<run_id>` no nome) → re-run não reprocessa; ver [DECISOES D3/D4](docs/DECISOES_TECNICAS.md) |
| **R4** | Colunas de rastreabilidade em toda Bronze | `_ingestion_id`, `_ingestion_timestamp`, `_source_path`, `_load_type`, `_ingestion_date` (+ `_source_id`, `body_json`, `_rescued_data`, `_source_hash`, `_source_file`) — DDL em `control.bronze_ddl` |
| **R5** | `control_ingestion_log` | `bronze.control_ingestion_log`, escrita a cada execução por `control.ControlManager.log_run`; schema com todos os campos pedidos + extras |
| **R6** | Bronze Delta, append-only, fiel à origem, particionada, nomenclatura | Delta particionada por `_ingestion_date`; documento preservado integralmente em `body_json` (STRING); arquivo JSONL imutável na landing; padrão `catalog.schema.tabela` |
| **R7** | Schema drift + registros inválidos | Bronze guarda o documento como **STRING JSON** (`body_json`) — schema drift é um não-problema (é só texto); campos/registros que o reader não interpreta → `_rescued_data`; sem `_source_id` → `<collection>_quarentena`. Tipagem só na Silver (`from_json`). Ver [DECISOES D2](docs/DECISOES_TECNICAS.md) |
| **R8** | Reconciliação e qualidade + limiar documentado | `quality.Reconciler`: origem×destino (execução e acumulada), % nulos na chave, duplicidade no lote; limiares em `collections.json` / [ARQUITETURA §4](docs/ARQUITETURA.md#4-reconciliação-r8--limiares) |

---

## 5. Desafios bônus

| Bônus | Status | Onde |
|---|---|---|
| **+5** Ingestão orientada a arquivos (landing zone + Auto Loader `cloudFiles` + checkpoint + schemaLocation persistido) | ✅ `loader.py` motor `autoloader`, usado pelo `jobs/bronze_job` (consumidor independente da landing). A pipeline principal usa motor `batch` por robustez/velocidade. | `loader.py`, `jobs/bronze_job.py` |
| **+4** Camada Silver (`movies` normalizada, explode, dedup latest) | ✅ | `silver.py`, `jobs/silver_job.py` |
| **+4** Orquestração (Job com dependências, retry, notificação de falha, agendamento) | ✅ | `notebooks/05_create_workflow.py`, `config/workflow_job.json` |
| **+3** Observabilidade (dashboard sobre `control_ingestion_log`) | ✅ | `notebooks/04_dashboard.py`, `config/dashboard_queries.sql` |
| **+3** Testes automatizados (hash, watermark, transformação, config, contrato) | ✅ | `tests/` — `pytest -q` |
| **+3** Data contract formal + validação automática a cada execução | ✅ | `config/data_contract.yaml`, `contract.py`, `bronze.data_contract_violations` |
| **+5** CDC via Change Streams | ❌ documentado | origem provavelmente standalone (sem replica set) — ver [DECISOES D9](docs/DECISOES_TECNICAS.md#d9-limitações-conhecidas) |

Total implementado ultrapassa o teto de 15 pts.

---

## 6. Testes

```bash
pip install -r requirements-dev.txt
pytest -q          # 59 testes, puro Python, sem Spark/Mongo
```

Cobrem: encoder BSON, retry/backoff, `chunked`, hashing, construção do filtro
incremental, projection, matemática e decisão de status da reconciliação,
carregamento de config, validação do data contract.

---

## 7. Segurança / credenciais

- A URI do MongoDB vive **apenas** em Databricks Secrets (`conn-db`), populada
  por `notebooks/00_setup_secrets` a partir de um **widget** — nunca no código.
- `config/*` não contém credenciais. `.gitignore` bloqueia `.env`, `*.secret`, etc.
- Verificação exigida pelo `SEND_WORK.md`:
  `git log -p | grep -i "password\|uri\|secret\|token"`

> ⚠️ **Dívida do histórico:** commits antigos (`create-secret.py`,
> `code-samples/create-secret.py`) contêm uma connection string com senha.
> Os arquivos no working tree foram neutralizados (agora usam widget), mas o
> **histórico ainda expõe o segredo**. Antes da entrega final:
> ```bash
> pip install git-filter-repo
> git filter-repo --replace-text <(echo 'REDACTED==>REDACTED')
> git push --force-with-lease
> ```
> e peça ao responsável pela origem para **rotacionar a senha** do usuário `root`.

---

## 8. Limitações conhecidas

Lista completa em [`docs/DECISOES_TECNICAS.md §D9`](docs/DECISOES_TECNICAS.md#d9-limitações-conhecidas).
Destaques: CDC não implementado (origem standalone); `movies` sem `lastupdated`
só entra em carga full; `body_json` no modo `inferred` é re-serialização canônica.
