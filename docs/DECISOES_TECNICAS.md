# Decisões Técnicas e Justificativas

Complemento do [README](../README.md) e da [ARQUITETURA](ARQUITETURA.md).
Foca no *porquê* de cada escolha e nos trade-offs.

---

## D1. Landing zone (Volume) + dois motores de carga

**Escolha:** MongoDB → arquivo JSONL num Volume → Bronze. A leitura landing→Bronze
tem **dois motores** (`config: autoloader.engine`):

| Motor | Como | Quando |
|---|---|---|
| `batch` (**padrão**) | `spark.read.json()` dos arquivos **daquela execução** (identificados pelo `<run_id>` no nome) → re-serializado em `body_json` STRING (`to_json(struct(*campos))`) → append/MERGE | pipeline principal (`ingestion_job`) |
| `autoloader` | `readStream` `cloudFiles` + `checkpointLocation` + `schemaLocation` persistido + `trigger(availableNow)` → mesmo `body_json` STRING | `bronze_job` (reprocessamento / bônus +5) |

**Por que a landing zone (comum aos dois):**
- Desacopla extração de carga. A Bronze pode ser reconstruída sem tocar na origem.
- O `.jsonl` imutável no Volume **é** a prova de fidelidade à origem (R6).

**Por que `batch` como padrão:**
- Volume real do trabalho é pequeno (~80k linhas no total das 6 coleções). O
  streaming do Auto Loader cobra *cold start* por query (schemaLocation,
  checkpoint, provisão) — e a pipeline sobe **uma query por coleção, em série**.
  Em Serverless isso levou ~20 min / travou.
- `batch` lê exatamente os arquivos da execução (`ext.files`), sem checkpoint:
  a idempotência vem da watermark (incremental não re-extrai) e do `MERGE` por
  `_source_id` (full). Roda as 6 coleções em ~1–2 min.
- A fidelidade **byte-a-byte** à origem vive no arquivo `.jsonl` da landing
  (`json.dumps(doc, default=bson_default)`, nunca reescrito). `body_json` na
  Bronze é uma **re-serialização canônica**: o `spark.read.json()` infere um
  schema por lote e `to_json(struct(...))` reconstrói o JSON a partir dele —
  mesmo conteúdo, mas ordem de chaves/formatação podem diferir do arquivo
  original. Ver D9.

**Por que manter o `autoloader`:**
- Atende literalmente o bônus **+5** ("consumir com Auto Loader / `readStream` com
  checkpoint e schema inference persistida"). Fica no `bronze_job`, fora do
  caminho crítico, então a lentidão/fragilidade não bloqueia a entrega.

**Trade-off:** o `batch` não tem "checkpoint" formal — se a mesma execução for
disparada 2× com o mesmo `run_id` (não acontece na prática, cada `run_pipeline`
gera um UUID novo), poderia reprocessar. Mitigado por `MERGE`/watermark.

---

## D2. Documento inteiro em `body_json STRING` como forma canônica da Bronze

**Escolha:** cada documento é ingerido numa única coluna `body_json STRING`
(JSON do documento completo). `_rescued_data` guarda o que o reader não
interpretou. A Bronze tem 10 colunas fixas, só `STRING`/`TIMESTAMP`/`DATE`.

**Por quê:**
- `sample_mflix` é deliberadamente heterogêneo (`imdb`/`tomatoes`/`awards`
  ausentes, `year` às vezes texto). Um `StructType` fixo na Bronze exigiria
  `mergeSchema` + reprocessamento a cada variação.
- Sendo texto, o schema da Bronze **nunca muda** — nenhuma evolução da origem é
  capaz de quebrar a carga (R7).
- Fidelidade: nenhum campo é perdido, nenhum é renomeado (R6).
- A tipagem acontece na Silver, com `get_json_object` / `from_json` campo a
  campo — onde o custo de um campo novo é uma linha de código, não um
  reprocessamento da Bronze.

**`VARIANT` foi avaliado e descartado.** Era a escolha inicial (`body_variant
VARIANT` ao lado do `body_json`) e é o tipo "certo" no papel: queryável via
`body_variant:imdb:rating::double`, sem achatar nada na ingestão. Na prática, no
Serverless deste workspace o `CREATE TABLE ... VARIANT` funciona mas **qualquer
leitura falha**:

```
[INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from "body_variant".
Need a complex type [STRUCT, ARRAY, MAP] but got "VARIANT". SQLSTATE: 42000
```

— tanto na sintaxe de dois-pontos (`body_variant:_id`) quanto na de colchetes
(`body_variant['_id']`). `STRING` é universalmente legível, igualmente lossless,
e `get_json_object` roda em qualquer runtime. Trocamos açúcar sintático por uma
pipeline que executa.

**Requisito resultante:** nenhum tipo exótico — a Bronze roda em qualquer
DBR 14.3 LTS+ com Unity Catalog.

---

## D3. Watermark persistida em tabela Delta, não em arquivo

**Escolha:** `bronze.ingestion_watermark`, uma linha por coleção, `MERGE` upsert.

**Por quê:** transacional (ACID), versionada (`DESCRIBE HISTORY`), consultável
junto do `control_ingestion_log`. Um arquivo `.json` no Volume não dá rollback
nem histórico.

**Regra de avanço:** a watermark só avança quando `safe_to_advance_watermark`
(SUCCESS, ou PARTIAL que **não** seja shortfall). Em falha/shortfall ela fica
parada e a próxima execução reprocessa a janela — preferimos reprocessar
(idempotente) a pular registro.

**`$gt` e não `$gte`:** evita re-trazer o último registro já gravado. O risco de
perder um registro com timestamp *exatamente* igual à watermark existe apenas se
dois documentos tiverem o mesmo `date` no milissegundo E a execução anterior
tiver lido só um deles — cenário coberto pela reconciliação (divergência) e pela
recomendação de um *full reconcile* periódico.

---

## D4. Idempotência: append (incremental) + MERGE (full)

Ver [ARQUITETURA §3](ARQUITETURA.md#3-decisões-técnicas). Resumo do raciocínio:

- **Incremental append:** a Bronze é a verdade *append-only*; cada chegada é um
  fato datado. Duplicata só aparece no cenário de crash entre gravar arquivo e
  persistir watermark — detectada (PARTIAL) e resolvida a jusante (Silver dedup).
- **Full MERGE:** `users`/`theaters`/etc. são recarregadas inteiras a cada run;
  sem MERGE a tabela dobraria de tamanho por execução. `MERGE ... _source_id`
  mantém exatamente 1 linha por documento e atualiza `_ingestion_*`.
- O enunciado lista explicitamente "append + dedup por chave/hash, MERGE,
  partição sobrescrita" como estratégias aceitáveis — usamos as duas primeiras
  conforme o modo de carga.

---

## D5. Reconciliação: limiares e o que é FAILED vs PARTIAL

| Situação | Decisão | Motivo |
|---|---|---|
| `_source_id` nulo | **FAILED** | chave de linhagem é inegociável (R8: "nunca nulo") |
| destino < origem, div > 5% | **FAILED** | perda sistemática — algo quebrou no meio |
| destino < origem, div ≤ 5% | **PARTIAL**, watermark parada | provável corrida; reprocessa |
| destino > origem (over) | **PARTIAL**, watermark avança | duplicata é chata mas não é perda; Silver resolve |
| duplicidade `_source_id` no lote | **PARTIAL** | idem |
| contrato violado | **PARTIAL** | dado entrou, mas fora do contrato — visível para correção |

Limiar por coleção em `collections.json` (`reconciliation_threshold_pct`).
`comments` usa 0.5% (volume alto, esperamos precisão); full usa 0% (recarga
completa deve bater exatamente).

---

## D6. Projection obrigatória de campos sensíveis/largos

| Coleção | Campo | Motivo |
|---|---|---|
| `users` | `password` | hash bcrypt, sem valor analítico, sensível |
| `sessions` | `jwt` | token ativo — risco de segurança |
| `embedded_movies` | `plot_embedding` | ~1536 floats ≈ 12 KB/doc (~42 MB total) — memória (R2) |
| `movies` | `fullplot`, `poster` | campos largos sem uso na Bronze |

Feito no **servidor** (`find(projection=...)`), não depois no Spark — o dado
sensível nunca chega ao Databricks.

---

## D7. Coleção vazia (`sessions`) não quebra a pipeline

`allow_empty=true` → `count==0` gera `ExtractResult(empty=True)` → log
`status=SUCCESS`, `qtd_lida_origem=0`, nenhum arquivo escrito, nenhuma exceção.
O mesmo caminho serve para "incremental sem novidades" (evidência 2).

---

## D8. `_ingestion_id` único por execução, compartilhado entre coleções

Uma invocação da pipeline = um `run_id` (UUID). O `control_ingestion_log` tem uma
linha por `(run_id, collection)`; a Bronze marca cada registro com o `run_id`.
As "3 execuções obrigatórias" são 3 `run_id` distintos. Facilita responder
"o que a execução X carregou?" com um único `WHERE _ingestion_id = ...`.

---

## D9. Limitações conhecidas

- **CDC real (Change Streams)** não implementado: o servidor de origem
  (`directConnection=true`, standalone) provavelmente não é replica set, o que
  Change Streams exige. A watermark por campo de data cobre o caso de uso.
- **`movies.lastupdated`**: documentos criados **sem** o campo após a 1ª carga
  não são capturados pela incremental (filtro `$exists:true`). Mitigação:
  `force_full=true` periódico em `movies`.
- **`body_json` é sempre uma re-serialização canônica** (`to_json(struct(...))`
  a partir do schema inferido pelo `spark.read.json()`/Auto Loader) — ordem de
  chaves e formatação podem diferir do documento original. A fidelidade
  byte-a-byte fica garantida no arquivo `.jsonl` imutável da landing, não em
  `body_json`. Não existe um modo alternativo "byte-perfeito" na Bronze desde
  que o `VARIANT` foi removido (ver D2).
- **Reconciliação acumulada com MERGE**: para coleções full, `COUNT(*)` da Bronze
  é `≤` à soma do control log (upserts não somam) — tratado como `accumulated_ok`
  quando `bronze >= soma` é falso apenas se `bronze < soma`.
- Sem *file notification* no Auto Loader (usa directory listing) — suficiente
  para o volume do trabalho; para produção com muitos arquivos, ligar
  `cloudFiles.useNotifications`.

---

## D10. MERGE com lista explícita de colunas, nunca `UpdateAll`/`InsertAll`

**Escolha:** `loader._merge` monta `assign = {c: F.col(f"s.{c}") for c in BRONZE_COLUMNS}`
e usa `whenMatchedUpdate(set=assign)` / `whenNotMatchedInsert(values=assign)` — nunca
`whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()`.

**Por quê:** `UpdateAll`/`InsertAll` expandem `*` pelas colunas do **alvo** (a tabela
Delta já existente). Se a tabela física tiver uma coluna que o DataFrame de
origem não tem — por exemplo `body_variant`, sobra de um DDL anterior à remoção
do `VARIANT` (D2), ou qualquer coluna que uma migração futura adicione sem
recriar a tabela — o Spark não consegue resolver a expressão e o `MERGE` inteiro
falha (`[DELTA_MERGE_UNRESOLVED_EXPRESSION]`), mesmo a Bronze estando com dado
saudável. Aconteceu na prática: uma tabela `bronze.users` criada antes da
remoção do `VARIANT` quebrava o `MERGE` até o Git Folder ser re-sincronizado e o
schema recriado.

Com a lista de colunas explícita (`BRONZE_COLUMNS`, a mesma ordem do DDL em
`control.bronze_ddl`), colunas extra/legadas do alvo são simplesmente ignoradas
— o `MERGE` sempre grava exatamente as 10 colunas de rastreabilidade, não
importa o que sobrou na tabela física. Ainda existe um `try/except` de
segurança: se mesmo assim o `MERGE` falhar, o loader cai para `overwrite` do
snapshot (log `WARNING`, nunca perde a execução).

**Risco residual:** `control.set_watermark` (tabela de watermark) ainda usa
`whenMatchedUpdateAll()`/`whenNotMatchedInsertAll()`, porque a origem desse
`MERGE` é sempre construída com o `WATERMARK_SCHEMA` fixo do próprio código
(nunca varia com o dado do Mongo) — hoje seguro. Se `WATERMARK_SCHEMA` for
alterado no futuro sem recriar `bronze.ingestion_watermark`, o mesmo problema
se repete ali.

---

## D11. ANSI mode e try_cast com limpeza na tipagem da Silver (R7)

**Escolha:** as colunas numéricas de `silver.movies` (`year`, `runtime`,
`imdb.*`, `tomatoes.*`, `awards.*`, `num_mflix_comments`) passam por um helper
(`silver._jc`) que extrai o primeiro token numérico do valor bruto via
`regexp_extract` **antes** de castar, em vez de um `.cast(tipo)` direto.

**Por quê:** o Databricks roda em **ANSI SQL mode**. Sob ANSI, `CAST` de uma
string que não representa um número válido **lança exceção**
(`[CAST_INVALID_INPUT]`) em vez de virar `NULL` como no Spark legado — e um
`CAST` inválido em uma única linha derruba a escrita da tabela inteira.
`sample_mflix` é schemaless por natureza (R7) e tem pelo menos um documento
real com `movies.year = '1981è'` (provável range de ano com o traço corrompido
por encoding, ex. um `–` mal decodificado). Um `try_cast` simples resolveria o
crash, mas devolveria `NULL` — perdendo um dado recuperável.

Em vez disso, `_jc` limpa antes de castar: `regexp_extract(valor, '-?\d+\.?\d*')`
extrai o primeiro número dentro da string suja (`'1981è'` → `'1981'` → `1981`),
e só cai em `NULL` se não houver nenhum dígito no valor (nada a recuperar). Os
campos de timestamp (`released`, `comments.date`) usam `try_to_timestamp`
(helper `_jt`) pela mesma razão — nesses o dado de origem já chega limpo
(ISO 8601 via `bson_default`/`.isoformat()`), então o `try_` é rede de
segurança, não limpeza ativa.

**Coerência com R7:** o documento bruto nunca é alterado — `body_json`
continua com `'1981è'` intacto; só a coluna **tipada** da Silver é que reflete
o valor limpo (ou `NULL` quando irrecuperável). Nenhum registro é descartado.

---

## D12. `force_full` precisa forçar MERGE também nas coleções incrementais

**Bug encontrado (2026-08-30, reset completo do ambiente):** rodar a pipeline
com `force_full=True` fazia `movies`/`comments` (coleções `incremental`)
reextraírem a coleção **inteira** da origem, mas `loader._write_mode` decidia
`append` vs `MERGE` olhando só `spec.load_mode` — o modo **estático** da
config (`collections.json`), que nunca muda em runtime. Resultado: toda
execução com `force_full=True` sobre `movies`/`comments` fazia **append da
coleção inteira de novo**, duplicando `_source_id` a cada rodada. Ficou visível
depois de um `DROP CATALOG` + múltiplas tentativas de `force_full=True` (viu-se
o dropdown do widget não "pegar" na primeira tentativa, ver conversa anterior)
— cada tentativa bem-sucedida duplicou de novo as ~23k/~50k linhas.

**Escolha:** `BronzeLoader.load()`/`_load_batch`/`_load_autoloader` passam a
receber `force_full` explicitamente, e `_write_mode(spec, force_full)` retorna
`merge` sempre que `force_full=True`, independente de `spec.load_mode`.
`pipeline.run_pipeline` propaga o `force_full` do parâmetro da execução direto
pro `loader.load(...)`.

**Por quê MERGE e não append-com-dedup-depois:** `force_full` existe
justamente como ferramenta de backfill/reset (ver D3) — sempre que ele reextrai
a coleção inteira, o destino tem que fazer *upsert* por `_source_id`, senão o
próprio propósito da ferramenta (reprocessar sem duplicar) fica quebrado. Isso
alinha `force_full` com o comportamento das coleções `full` "de verdade"
(`users`/`theaters`/etc., que já sempre MERGE — D4), que é exatamente o que
`force_full` simula para as incrementais.

**Limpeza necessária:** esse fix impede duplicação em execuções **futuras**,
mas não desfaz duplicatas que já foram gravadas por execuções `force_full`
anteriores a este commit. Se `bronze.movies`/`bronze.comments` já têm
`_source_id` duplicado, a única correção é recriar a tabela do zero
(`DROP CATALOG ... CASCADE` + `01_setup_catalog` + nova carga) — não há como
"consertar" uma tabela append-only com duplicatas sem reprocessar.

**Continuação do fix (mesma varredura, dois pontos que ficaram para trás na
primeira correção):**

- `loader._shape(..., load_type)` — chamado em `_load_batch` e
  `_load_autoloader` — recebia `spec.load_mode` (estático) em vez de
  `spec.effective_load_type(force_full)`. Consequência: a coluna `_load_type`
  gravada em CADA LINHA da Bronze (coluna de rastreabilidade obrigatória, R4)
  ficava `incremental` mesmo numa execução `force_full=True`, contradizendo o
  `control_ingestion_log` (que já mostrava `full` corretamente via
  `ControlRecord.load_type`). Corrigido para usar `effective_load_type`.
- `quality.Reconciler.evaluate` não recebia `force_full` — o campo
  `accumulated_ok` (diagnóstico informativo, não afeta `status`) assumia
  `append` para `movies`/`comments` mesmo depois de um `force_full` ter feito
  `MERGE`, podendo acusar divergência acumulada falsa. `evaluate()` agora
  aceita `force_full` e `pipeline.run_pipeline` propaga o parâmetro.
