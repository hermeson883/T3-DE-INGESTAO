# Trabalho Final — Ingestão Moderna de Dados

**Disciplina:** Engenharia de Dados / Ingestão Moderna de Dados
**Formato:** grupos de até 3 alunos
**Entrega:** repositório Git + apresentação técnica (15 min por grupo)
**Peso:** 100 pontos (+ até 15 pontos bônus)

---

## 1. Objetivo

Desenvolver uma **pipeline de ingestão de dados** que extraia as coleções do banco `sample_mflix` (MongoDB Atlas) e as materialize em um **Data Lake — camada Bronze**, aplicando boas práticas de uso de recursos computacionais e garantindo **rastreabilidade completa** de cada registro ingerido.

O trabalho avalia a capacidade do grupo de tomar decisões de engenharia (não apenas escrever código que "funciona") e de justificá-las tecnicamente.

---

## 2. Fonte de dados

Banco `sample_mflix` do MongoDB Atlas (cluster gratuito M0 é suficiente).

| Coleção | Volume aprox. | Característica relevante |
|---|---|---|
| `movies` | ~21.000 | schema heterogêneo, campos aninhados (`imdb`, `tomatoes`, `awards`), arrays (`cast`, `genres`), campo `lastupdated` |
| `comments` | ~50.000 | maior volume, campo `date` (carga incremental natural) |
| `users` | ~185 | pequena, dimensão |
| `theaters` | ~1.500 | GeoJSON aninhado |
| `sessions` | poucos docs | tratar coleção pequena/vazia sem quebrar o pipeline |
| `embedded_movies` | ~3.500 | array de embeddings (campo largo, cuidado com memória) |

**Mínimo obrigatório:** ingerir **todas** as coleções acima com o mesmo código genérico e parametrizado. Não será aceito um notebook com um bloco copiado e colado por coleção.

---

## 3. Requisitos obrigatórios

### R1 — Pipeline parametrizada e genérica
- Um único componente de ingestão que receba, no mínimo: `database`, `collection`, `modo_carga` (`full` | `incremental`), `campo_watermark`, `destino`.
- Configuração externalizada (arquivo `.json`/`.yaml` ou widgets), **nunca hardcoded** no corpo do código.
- Código organizado em funções ou classes (OOP), com separação clara entre *extract*, *load* e *control*.

### R2 — Boas práticas de uso de recursos
O grupo deve demonstrar (e justificar no README) o uso de pelo menos **quatro** destas técnicas:
- Leitura paginada / em lotes (`batchSize`, cursor, particionamento por `_id`).
- **Projection / pushdown**: trazer apenas os campos necessários da origem, não o documento inteiro quando não for preciso.
- Controle de paralelismo e número de partições no destino (evitar *small files* e *skew*).
- Ausência de `collect()`, `toPandas()` ou `list(cursor)` sobre coleções grandes — justificar qualquer exceção.
- Reuso de conexão / connection pooling.
- *Retry* com *backoff* em falhas de rede da origem.

### R3 — Modos de carga
- **Full load** para coleções pequenas e estáveis (`users`, `theaters`).
- **Carga incremental** para pelo menos uma coleção grande (`comments` por `date`, ou `movies` por `lastupdated`), com **watermark persistida** entre execuções.
- O pipeline deve ser **idempotente**: rodar duas vezes seguidas não pode duplicar registros nem corromper a camada Bronze. Descreva a estratégia escolhida (append + dedup por chave/hash, `MERGE`, partição sobrescrita, etc.).

### R4 — Rastreabilidade (linhagem técnica)
Toda tabela Bronze deve conter, no mínimo, as colunas de controle abaixo:

| Coluna | Descrição |
|---|---|
| `_ingestion_id` | UUID da execução (run id) |
| `_ingestion_timestamp` | timestamp UTC da gravação |
| `_source_path` | ex.: `mongodb_atlas` |
| `_load_type` | `full` ou `incremental` |
| `_ingestion_date` | data (coluna de partição) |

### R5 — Tabela de controle de execuções
Criar uma tabela `bronze.control_ingestion_log` (ou equivalente) gravada a cada execução, contendo pelo menos:
`_ingestion_id`, `collection`, `load_type`, `watermark_inicial`, `watermark_final`, `qtd_lida_origem`, `qtd_gravada_destino`, `start_time`, `end_time`, `duracao_seg`, `status` (`SUCCESS` | `FAILED` | `PARTIAL`), `mensagem_erro`.

Essa tabela é a fonte de verdade para responder: *"o que foi carregado, quando, por qual execução e com qual resultado?"*

### R6 — Camada Bronze com fidelidade à origem
- Formato **Delta Lake** (ou Parquet, Avro se justificado).
- Bronze é **append-only** e preserva o dado **como veio**: sem regra de negócio, sem renomear campos de negócio, sem descartar colunas por conveniência.
- Estrutura de diretórios/catálogo padronizada, ex.:
  `bronze/sample_mflix/<collection>/_ingestion_date=YYYY-MM-DD/`
  Ou qualquer outro que seja mais aderente a arquitetura proposta.
- Nomenclatura consistente de catálogo/schema/tabela (documentar o padrão adotado).

### R7 — Tratamento de schema
- Documentar como a pipeline lida com **schema drift** (campo novo, campo ausente, tipo divergente entre documentos tipico de noSQL schemless).
- Analisar a Estratégia: *schema evolution* explícito (`mergeSchema`) **ou** persistência do documento como JSON/string bruta + colunas de controle. Justificar a escolha e seus impactos na camada Silver.
- Registros que não puderem ser convertidos devem ser preservados (coluna de *rescue*/quarentena), nunca descartados silenciosamente.

### R8 — Reconciliação e qualidade
Ao final de cada execução, o pipeline deve validar e registrar:
- Contagem na origem × contagem no destino (por execução e acumulada).
- Percentual de nulos nas chaves (`_source_id` nunca nulo).
- Duplicidade de `_source_id` dentro do mesmo lote.
- Execução deve falhar (ou marcar `PARTIAL`) quando a divergência ultrapassar o limiar definido pelo grupo — o limiar deve estar documentado.

---

## 4. Entregáveis

1. **Pull Request** contendo:
   - código-fonte organizado (`/src`, `/notebooks`, `/config`);
   - `README.md` com: arquitetura, decisões técnicas e justificativas, como executar, limitações conhecidas;
   - arquivo de configuração de exemplo (**sem credenciais**).
2. **Diagrama de arquitetura** da solução (origem → ingestão → Bronze → controle/observabilidade).
3. **Evidências de execução**: prints ou saída da `control_ingestion_log` com pelo menos **três execuções** — carga inicial, carga incremental sem novidades e carga incremental com dados novos.
5. **Registro de contribuição individual** (quem fez o quê). [ Importante ]

> Credenciais **não** podem ser versionadas. Uso de secrets/variáveis de ambiente é obrigatório — segredo exposto no repositório gera perda automática de pontos.
  Devido ao ambiente limitado. separa a criacao de secrets do projeto em notebook.

---

## 5. Rubrica de avaliação (100 pts)

| Critério | Pontos |
|---|---|
| Pipeline genérica, parametrizada e bem estruturada (R1) | 15 |
| Boas práticas de uso de recursos, com justificativa técnica (R2) | 15 |
| Carga incremental funcional + idempotência comprovada (R3) | 15 |
| Colunas de rastreabilidade completas e corretas (R4) | 10 |
| Tabela de controle de execuções (R5) | 10 |
| Camada Bronze bem modelada, particionada e fiel à origem (R6) | 15 |
| Tratamento de schema drift e registros inválidos (R7) | 10 |
| Reconciliação e validações de qualidade (R8) | 5 |
| README, diagrama e documentação das decisões | 5 |

**Critérios de perda de pontos:** credenciais expostas; código duplicado por coleção; uso de `collect()`/`toPandas()` sem justificativa; ausência de evidências de execução; membros do grupo sem contribuição identificável.

---

## 6. Desafios bônus (até 15 pts)

Escolha livre — implemente e documente:

- **(+5) CDC real:** ingestão via *MongoDB Change Streams* em vez de watermark por campo de data.
- **(+5) Ingestão orientada a arquivos:** exportar a origem para uma landing zone e consumir com *Auto Loader* / `readStream` com checkpoint e *schema inference* persistida.
- **(+4) Orquestração:** Job/Workflow com dependências, agendamento, *retry* e notificação de falha.
- **(+4) Camada Silver:** normalização de `movies` (explode de `cast`/`genres`, achatamento de `imdb`/`tomatoes`) com deduplicação por `_source_id` e *latest record*.
- **(+3) Observabilidade:** dashboard sobre a `control_ingestion_log` (volume por dia, duração, taxa de falha).
- **(+3) Testes automatizados:** testes unitários das funções de transformação e de geração de hash/watermark.
- **(+3) Data contract:** definição formal do contrato da origem e validação automática a cada execução.

Máximo de 15 pontos bônus, ainda que a soma dos itens escolhidos ultrapasse esse valor.

---