-- =============================================================================
-- Consultas do dashboard de observabilidade (bonus +3)
-- Fonte unica: mflix.bronze.control_ingestion_log
-- Substitua {{catalog}} por mflix (ou use um parametro de dashboard).
-- =============================================================================

-- 1) Volume ingerido por dia e por colecao ---------------------------------------
SELECT CAST(start_time AS DATE)          AS dia,
       collection,
       SUM(qtd_lida_origem)              AS lidos_origem,
       SUM(qtd_gravada_destino)          AS gravados_bronze,
       SUM(qtd_quarentena)               AS quarentena
FROM {{catalog}}.bronze.control_ingestion_log
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

-- 2) Duracao por execucao (tendencia) ------------------------------------------
SELECT start_time,
       collection,
       duracao_seg,
       qtd_gravada_destino,
       ROUND(qtd_gravada_destino / NULLIF(duracao_seg, 0), 1) AS linhas_por_seg
FROM {{catalog}}.bronze.control_ingestion_log
ORDER BY start_time DESC;

-- 3) Taxa de falha / partial por dia ------------------------------------------
SELECT CAST(start_time AS DATE) AS dia,
       COUNT(*)                                                   AS execucoes,
       SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END)        AS ok,
       SUM(CASE WHEN status = 'PARTIAL' THEN 1 ELSE 0 END)        AS partial,
       SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END)        AS failed,
       ROUND(100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_falha
FROM {{catalog}}.bronze.control_ingestion_log
GROUP BY 1
ORDER BY 1 DESC;

-- 4) Divergencia de reconciliacao por colecao (ultimas execucoes) -------------
SELECT collection, start_time, qtd_lida_origem, qtd_gravada_destino,
       divergencia_pct, qtd_duplicada_lote, status
FROM {{catalog}}.bronze.control_ingestion_log
WHERE divergencia_pct > 0 OR status <> 'SUCCESS'
ORDER BY start_time DESC;

-- 5) Progressao da watermark (carga incremental) -----------------------------
SELECT collection, watermark_field, watermark_value, watermark_type, updated_at
FROM {{catalog}}.bronze.ingestion_watermark
ORDER BY collection;

-- 6) Ultimo status por colecao (cartoes) -------------------------------------
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY collection ORDER BY start_time DESC) rn
  FROM {{catalog}}.bronze.control_ingestion_log
)
SELECT collection, load_type, status, qtd_lida_origem, qtd_gravada_destino,
       divergencia_pct, duracao_seg, end_time
FROM ranked WHERE rn = 1
ORDER BY collection;

-- 7) Violacoes de data contract -------------------------------------------------
SELECT collection, field, kind, detail, violation_pct, detected_at
FROM {{catalog}}.bronze.data_contract_violations
ORDER BY detected_at DESC;

-- 8) Reconciliacao acumulada: control_log x contagem real da Bronze ----------
--    (rode a parte "UNION ALL" adicionando as demais colecoes conforme necessario)
SELECT 'comments' AS collection,
       (SELECT SUM(qtd_gravada_destino) FROM {{catalog}}.bronze.control_ingestion_log
        WHERE collection = 'comments' AND status <> 'FAILED')          AS soma_control_log,
       (SELECT COUNT(*) FROM {{catalog}}.bronze.comments)              AS linhas_bronze;