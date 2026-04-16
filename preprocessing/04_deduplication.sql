-- ==============================================
-- 04_deduplication.sql
-- Identifies and removes duplicate event rows
-- from raw_events.
--
-- Duplicate audit results:
--   cart     — 71,966 groups, 126,996 rows to remove
--   view     —  3,606 groups,   3,669 rows to remove
--   purchase —     85 groups,      85 rows to remove
--   Total    — 75,657 groups, 130,750 rows to remove
--
-- Root cause:
--   cart     — event flooding from frontend logging
--              bug. Same product added to cart
--              multiple times per session at
--              identical timestamps. Worst case
--              78 occurrences in one session.
--   view     — page refreshes within same session
--              generating duplicate view events.
--   purchase — payment confirmation page refreshes.
--              Only 85 cases across 697,470
--              purchases (0.01%).
--
-- Strategy:
--   Keep exactly 1 row per unique combination of
--   event_time, event_type, product_id, user_id,
--   user_session using ROW_NUMBER() window function.
--   Delete all rows where row_number > 1.
-- ==============================================


-- ==============================================
-- SECTION 1: baseline duplicate audit
-- Documents the state before deduplication
-- ==============================================

SELECT
    event_type,
    COUNT(*)                AS duplicate_groups,
    SUM(occurrences)        AS total_duplicate_rows,
    SUM(occurrences) -
        COUNT(*)            AS rows_to_remove,
    MAX(occurrences)        AS worst_case,
    ROUND(AVG(occurrences),
        2)                  AS avg_occurrences
FROM (
    SELECT
        event_time,
        event_type,
        product_id,
        user_id,
        user_session,
        COUNT(*)            AS occurrences
    FROM raw_events
    GROUP BY
        event_time,
        event_type,
        product_id,
        user_id,
        user_session
    HAVING COUNT(*) > 1
) dupes
GROUP BY event_type
ORDER BY total_duplicate_rows DESC;


-- ==============================================
-- SECTION 2: preview rows that will be removed
-- Shows a sample of what deduplication removes
-- before committing to the delete
-- ==============================================

SELECT
    event_time,
    event_type,
    product_id,
    user_id,
    user_session,
    row_num
FROM (
    SELECT
        event_time,
        event_type,
        product_id,
        user_id,
        user_session,
        ROW_NUMBER() OVER (
            PARTITION BY
                event_time,
                event_type,
                product_id,
                user_id,
                user_session
            ORDER BY
                event_time
        ) AS row_num
    FROM raw_events
) ranked
WHERE row_num > 1
LIMIT 20;


-- ==============================================
-- SECTION 3: remove duplicates
-- Uses a CTE with ROW_NUMBER() to identify
-- duplicate rows, then deletes all but the first
-- occurrence of each duplicate group.
--
-- This will take several minutes on 109M rows.
-- ==============================================

WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY
                event_time,
                event_type,
                product_id,
                user_id,
                user_session
            ORDER BY
                event_time
        ) AS row_num
    FROM raw_events
)
DELETE FROM raw_events
WHERE ctid IN (
    SELECT ctid
    FROM ranked
    WHERE row_num > 1
);


-- ==============================================
-- SECTION 4: post-deduplication row count
-- Confirm expected rows were removed
-- Expected: 109,950,731 - 130,750 = 109,819,981
-- ==============================================

SELECT
    COUNT(*)                AS total_rows_after,
    109950731 - COUNT(*)    AS rows_removed
FROM raw_events;


-- ==============================================
-- SECTION 5: confirm no duplicates remain
-- Should return 0 rows
-- ==============================================

SELECT
    event_time,
    event_type,
    product_id,
    user_id,
    user_session,
    COUNT(*) AS occurrences
FROM raw_events
GROUP BY
    event_time,
    event_type,
    product_id,
    user_id,
    user_session
HAVING COUNT(*) > 1
LIMIT 10;


-- ==============================================
-- SECTION 6: deduplication summary
-- Documents what changed for the narrative
-- ==============================================

SELECT
    'cart duplicates removed'       AS action,
    126996                          AS rows_removed,
    'Event flooding — frontend bug' AS root_cause
UNION ALL
SELECT
    'view duplicates removed'       AS action,
    3669                            AS rows_removed,
    'Page refreshes same session'   AS root_cause
UNION ALL
SELECT
    'purchase duplicates removed'   AS action,
    85                              AS rows_removed,
    'Payment page refreshes'        AS root_cause;