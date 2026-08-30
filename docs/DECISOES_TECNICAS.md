# Decisões Técnicas e Justificativas

Complemento do [README](../README.md) e da [ARQUITETURA](ARQUITETURA.md).
Foca no *porquê* de cada escolha e nos trade-offs.

---

## D1. Landing zone + Auto Loader (em vez de Mongo → Bronze direto)

**Escolha:** MongoDB → arquivo JSONL num Volume → Auto Loader → Bronze.

**Por quê:**
- Desacopla extração de carga. A Bronze pode ser reconstruída/reprocessada
  (`jobs/bronze_job`) sem tocar de novo na origem.
- O arquivo imutável no Volume **é** a prova de fidelidade à origem (R6): posso
  abrir o `.jsonl` e comparar byte a byte.
- `cloudFiles` traz de graça: checkpoint (exactly-once por arquivo),
  `schemaLocation` (schema inferido persistido — bônus), `rescuedDataColumn`
  (schema drift sem quebrar — R7).
- Atende o bônus **+5 (ingestão orientada a arquivos)** como parte do desenho
  principal, não como um apêndice.

**Trade-off:** mais peças móveis (checkpoints, schema locations, um Volume) e uma
cópia extra dos dados na landing. Aceitável: o Volume é barato e a cópia é a
própria evidência de auditoria.

---

## D2. `VARIANT` (`single_variant`) como forma canônica da Bronze

**Escolha:** cada documento é ingerido numa única coluna `body_variant VARIANT`;
`body_json` é o `to_json(body_variant)`; `_rescued_data` reservada para o modo
`inferred`.

**Por quê:**
- `sample_mflix` é deliberadamente heterogêneo (`imdb`/`tomatoes`/`awards`
  ausentes, `year` às vezes texto). Um `StructType` fixo na Bronze exigiria
  `mergeSchema` + reprocessamento a cada variação.
- `VARIANT` é queryável (`body_variant:imdb:rating::double`) sem pagar o custo de
  achatar tudo na ingestão.
- Fidelidade: nenhum campo é perdido, nenhum é renomeado (R6).

**Alternativa suportada (`inferred`):** Auto Loader infere o schema, persiste em
`schemaLocation`, e `schemaEvolutionMode=rescue` joga campo novo/divergente em
`_rescued_data`. Útil se o avaliador quiser ver colunas tipadas já na Bronze.
Troca-se fidelidade estrutural por conveniência.

**Requisito:** `VARIANT` GA exige DBR 15.3+ / Serverless recente (o repo original
já usa `parse_json`/`schema_of_json_agg`, então o workspace suporta).

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
- **`body_json` no modo `inferred`** é uma re-serialização canônica (ordem de
  chaves pode diferir da origem); a fidelidade byte-a-byte fica no arquivo da
  landing. No modo `single_variant` (padrão) isso não se aplica.
- **Reconciliação acumulada com MERGE**: para coleções full, `COUNT(*)` da Bronze
  é `≤` à soma do control log (upserts não somam) — tratado como `accumulated_ok`
  quando `bronze >= soma` é falso apenas se `bronze < soma`.
- Sem *file notification* no Auto Loader (usa directory listing) — suficiente
  para o volume do trabalho; para produção com muitos arquivos, ligar
  `cloudFiles.useNotifications`.
