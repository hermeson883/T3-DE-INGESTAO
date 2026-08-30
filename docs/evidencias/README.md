# Evidências de execução

Gere estas evidências rodando **`notebooks/03_evidencias`** e salve os prints
aqui com **exatamente** estes nomes (o avaliador procura por eles):

| Arquivo | O que capturar | Cenário |
|---|---|---|
| `execucao_01_full_load.png` | saída da seção *Execução 1* — `control_ingestion_log` com as 6 coleções: `users`/`theaters`/`sessions`/`embedded_movies` com `load_type=full`; `movies`/`comments` com `load_type=incremental` extraindo o volume total (bootstrap) e gravando a watermark; `qtd_lida_origem` vs volumes documentados | carga inicial |
| `execucao_02_incremental_sem_novidades.png` | saída da seção *Execução 2* — `comments` com `qtd_lida_origem = 0`, `status = SUCCESS`, `watermark_inicial == watermark_final` | incremental sem novidades |
| `execucao_03_incremental_com_dados.png` | saída da seção *Execução 3* — `qtd_lida_origem = 3`, delta da Bronze = 3, `_source_id` duplicados = 0; + a sub-seção de idempotência (2ª rodada seguida = 0 lidos) | incremental com dados novos |

Opcionais (bônus):

| Arquivo | Conteúdo |
|---|---|
| `observabilidade.png` | dashboard de `notebooks/04_dashboard` |
| `workflow.png` | grafo do Job criado por `notebooks/05_create_workflow` |
| `silver.png` | `silver.movies` + `silver.movies_cast` de `jobs/silver_job` |
| `tests.png` | saída de `pytest -q` |

---

## Query única para o corpo do Pull Request

A última célula de `03_evidencias` roda:

```sql
SELECT _ingestion_id, collection, load_type, watermark_inicial, watermark_final,
       qtd_lida_origem, qtd_gravada_destino, duracao_seg, status, mensagem_erro
FROM mflix.bronze.control_ingestion_log
WHERE _ingestion_id IN ('<run1>', '<run2>', '<run3>')
ORDER BY start_time, collection;
```

Cole o resultado (tabela) no PR conforme pede o `SEND_WORK.md`.

> Os `.png` não estão versionados neste diretório ainda — são adicionados por quem
> executa a pipeline no Databricks.
