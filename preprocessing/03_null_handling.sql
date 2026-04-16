-- ==============================================
-- 03_null_handling.sql
-- Identifies and handles null values across
-- all columns in raw_events.
--
-- Null audit results (109,950,743 total rows):
--   category_code  — 35,413,780 nulls (32.2%)
--   brand          — 15,331,243 nulls (13.9%)
--   user_session   —          12 nulls (0.0001%)
--   All other columns — 0 nulls
--
-- Strategy:
--   category_code  — fill with 'uncategorised'
--                    dropping 32% of rows would
--                    destroy funnel analysis
--   brand          — fill with 'unknown_brand'
--                    preserves as distinct segment
--   user_session   — drop 12 rows, sessionless
--                    rows cannot be tracked
-- ==============================================


-- ==============================================
-- SECTION 1: baseline null audit
-- Run before any changes to document the
-- raw state of the data
-- ==============================================

SELECT
    COUNT(*)                                AS total_rows,
    COUNT(*) - COUNT(category_code)         AS null_category_code,
    COUNT(*) - COUNT(brand)                 AS null_brand,
    COUNT(*) - COUNT(user_session)          AS null_user_session
FROM raw_events;


-- ==============================================
-- SECTION 2: inspect null category_code rows
-- Understand what types of events are missing
-- category codes before deciding how to handle
-- ==============================================

SELECT
    event_type,
    COUNT(*)                                AS null_category_rows,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*) FROM raw_events
        WHERE category_code IS NULL), 2)    AS pct_of_nulls
FROM raw_events
WHERE category_code IS NULL
GROUP BY event_type
ORDER BY null_category_rows DESC;


-- ==============================================
-- SECTION 3: inspect null brand rows
-- ==============================================

SELECT
    event_type,
    COUNT(*)                                AS null_brand_rows,
    ROUND(COUNT(*) * 100.0 / (
        SELECT COUNT(*) FROM raw_events
        WHERE brand IS NULL), 2)            AS pct_of_nulls
FROM raw_events
WHERE brand IS NULL
GROUP BY event_type
ORDER BY null_brand_rows DESC;


-- ==============================================
-- SECTION 4: handle nulls
-- Fill category_code and brand nulls
-- Drop the 12 sessionless rows
-- ==============================================

-- Fill null category_code
UPDATE raw_events
SET category_code = 'uncategorised'
WHERE category_code IS NULL;

-- Fill null brand
UPDATE raw_events
SET brand = 'unknown_brand'
WHERE brand IS NULL;

-- Drop sessionless rows
DELETE FROM raw_events
WHERE user_session IS NULL;


-- ==============================================
-- SECTION 5: post-clean null audit
-- Confirm all nulls have been resolved
-- All columns should return 0
-- ==============================================

SELECT
    COUNT(*)                                AS total_rows,
    COUNT(*) - COUNT(event_time)            AS null_event_time,
    COUNT(*) - COUNT(event_type)            AS null_event_type,
    COUNT(*) - COUNT(product_id)            AS null_product_id,
    COUNT(*) - COUNT(category_id)           AS null_category_id,
    COUNT(*) - COUNT(category_code)         AS null_category_code,
    COUNT(*) - COUNT(brand)                 AS null_brand,
    COUNT(*) - COUNT(price)                 AS null_price,
    COUNT(*) - COUNT(user_id)               AS null_user_id,
    COUNT(*) - COUNT(user_session)          AS null_user_session
FROM raw_events;


-- ==============================================
-- SECTION 6: null handling summary
-- Documents what changed for the narrative
-- ==============================================

SELECT
    'category_code filled'  AS action,
    35413780                AS rows_affected,
    'uncategorised'         AS fill_value
UNION ALL
SELECT
    'brand filled'          AS action,
    15331243                AS rows_affected,
    'unknown_brand'         AS fill_value
UNION ALL
SELECT
    'user_session dropped'  AS action,
    12                      AS rows_affected,
    'NULL — untrackable'    AS fill_value;