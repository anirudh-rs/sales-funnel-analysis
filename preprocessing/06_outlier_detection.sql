-- ==============================================
-- 06_outlier_detection.sql
-- Detects and flags price outliers in raw_events
-- using the IQR method.
--
-- Price profile findings:
--   Min:     0.00      Max:    2,574.07
--   P25:    67.96      P75:      360.11
--   Median: 164.88     P95:    1,003.85
--   Avg:    291.64     P99:    1,688.85
--   IQR:    292.15
--
-- IQR outlier boundaries (1.5 x IQR method):
--   Lower fence: 67.96 - (1.5 x 292.15) = -370.27
--   Upper fence: 360.11 + (1.5 x 292.15) =  798.34
--
-- Additional findings:
--   Zero price rows:     256,657  (likely unpriced)
--   Negative price rows:       0  (clean)
--   Rows above 1,000:  5,743,602
--   Rows above 5,000:          0
--
-- Strategy:
--   Flag outliers rather than delete them.
--   Deleting 5.7M rows would distort the funnel.
--   Instead add is_price_outlier and
--   is_zero_price columns so analysis queries
--   can include or exclude them as needed.
-- ==============================================


-- ==============================================
-- SECTION 1: calculate IQR boundaries
-- Confirms the outlier thresholds before
-- applying any flags
-- ==============================================

WITH price_stats AS (
    SELECT
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price)   AS p25,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price)   AS p75
    FROM raw_events
    WHERE price > 0
)
SELECT
    ROUND(p25::NUMERIC, 2)                  AS p25,
    ROUND(p75::NUMERIC, 2)                  AS p75,
    ROUND((p75 - p25)::NUMERIC, 2)          AS iqr,
    ROUND((p25 - 1.5 *
        (p75 - p25))::NUMERIC, 2)           AS lower_fence,
    ROUND((p75 + 1.5 *
        (p75 - p25))::NUMERIC, 2)           AS upper_fence
FROM price_stats;


-- ==============================================
-- SECTION 2: count rows outside IQR boundaries
-- Shows exactly how many rows will be flagged
-- ==============================================

WITH price_stats AS (
    SELECT
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price)   AS p25,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price)   AS p75
    FROM raw_events
    WHERE price > 0
)
SELECT
    COUNT(*) FILTER (
        WHERE price > (
            SELECT p75 + 1.5 * (p75 - p25)
            FROM price_stats))              AS above_upper_fence,
    COUNT(*) FILTER (
        WHERE price = 0)                    AS zero_price,
    COUNT(*) FILTER (
        WHERE price > (
            SELECT p75 + 1.5 * (p75 - p25)
            FROM price_stats)
        OR price = 0)                       AS total_to_flag
FROM raw_events;


-- ==============================================
-- SECTION 3: outlier breakdown by event type
-- Shows which funnel stages are most affected
-- ==============================================

WITH price_stats AS (
    SELECT
        PERCENTILE_CONT(0.25)
            WITHIN GROUP (ORDER BY price)   AS p25,
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (ORDER BY price)   AS p75
    FROM raw_events
    WHERE price > 0
)
SELECT
    event_type,
    COUNT(*) FILTER (
        WHERE price = 0)                    AS zero_price,
    COUNT(*) FILTER (
        WHERE price > (
            SELECT p75 + 1.5 * (p75 - p25)
            FROM price_stats))              AS above_fence,
    COUNT(*)                                AS total_rows,
    ROUND(COUNT(*) FILTER (
        WHERE price = 0) * 100.0 /
        COUNT(*), 2)                        AS zero_pct
FROM raw_events
GROUP BY event_type
ORDER BY total_rows DESC;


-- ==============================================
-- SECTION 4: add outlier flag columns
-- is_price_outlier — price above upper fence
-- is_zero_price    — price is exactly 0.00
-- ==============================================

ALTER TABLE raw_events
    ADD COLUMN IF NOT EXISTS is_price_outlier   BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS is_zero_price      BOOLEAN DEFAULT FALSE;


-- ==============================================
-- SECTION 5: populate outlier flags
-- Uses the IQR upper fence of 798.34
-- This will take several minutes on 109M rows
-- ==============================================

UPDATE raw_events
SET
    is_price_outlier = (price > 797.83),
    is_zero_price    = (price = 0.00);


-- ==============================================
-- SECTION 6: verify flags
-- Confirms flag counts match Section 2
-- ==============================================

SELECT
    COUNT(*) FILTER (
        WHERE is_price_outlier = TRUE)      AS flagged_outliers,
    COUNT(*) FILTER (
        WHERE is_zero_price = TRUE)         AS flagged_zero_price,
    COUNT(*) FILTER (
        WHERE is_price_outlier = FALSE
        AND is_zero_price = FALSE)          AS clean_price_rows,
    COUNT(*)                                AS total_rows
FROM raw_events;


-- ==============================================
-- SECTION 7: outlier detection summary
-- ==============================================

SELECT
    'IQR upper fence'           AS metric,
    '797.83'                    AS value,
    'P75 + 1.5 x IQR'          AS method
UNION ALL
SELECT
    'Price outliers flagged'    AS metric,
    '9,617,652'                 AS value,
    'price > 797.83'            AS method
UNION ALL
SELECT
    'Zero price flagged'        AS metric,
    '256,657'                   AS value,
    'price = 0.00'              AS method
UNION ALL
SELECT
    'Rows deleted'              AS metric,
    '0'                         AS value,
    'Flag only — not deleted'   AS method;