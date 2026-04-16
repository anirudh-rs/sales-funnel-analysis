-- ==============================================
-- 07_master_clean.sql
-- Creates the master clean view that all
-- analysis queries build from.
--
-- This view applies all preprocessing decisions
-- made in scripts 03-06:
--   03 — nulls filled/dropped
--   04 — duplicates removed
--   05 — dates normalised
--   06 — outliers flagged
--
-- Clean data profile:
--   Total rows:        109,819,981
--   Unique users:        5,316,649
--   Unique sessions:    23,016,650
--   Unique products:       206,876
--   Unique brands:           4,304
--   Unique categories:         130
--   Unique weeks:                9
--   Views:             104,331,840
--   Carts:               3,828,438
--   Purchases:           1,659,703
--   Price outliers:      9,617,652
--   Zero prices:           256,657
--
-- Usage:
--   All analysis queries use this view:
--   SELECT ... FROM vw_clean_events WHERE ...
--
--   To exclude price outliers:
--   WHERE is_price_outlier = FALSE
--
--   To exclude zero prices:
--   WHERE is_zero_price = FALSE
--
--   To analyse clean prices only:
--   WHERE is_price_outlier = FALSE
--   AND is_zero_price = FALSE
-- ==============================================


-- ==============================================
-- SECTION 1: drop and recreate the view
-- Safe to re-run at any time — view is rebuilt
-- from raw_events which holds the clean data
-- ==============================================

DROP VIEW IF EXISTS vw_clean_events;

CREATE VIEW vw_clean_events AS
SELECT
    -- Core event fields
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session,

    -- Derived date fields (from 05_date_normalisation)
    event_date,
    event_week,
    event_month,
    day_of_week,
    hour_of_day,
    is_weekend,
    is_spillover,

    -- Quality flags (from 06_outlier_detection)
    is_price_outlier,
    is_zero_price,

    -- Derived funnel stage
    -- Maps raw event_type to the 7-stage funnel
    CASE event_type
        WHEN 'view'     THEN 'Stage 2 — View'
        WHEN 'cart'     THEN 'Stage 4 — Cart'
        WHEN 'purchase' THEN 'Stage 6 — Purchase'
        ELSE 'Unknown'
    END                             AS funnel_stage,

    -- Derived price tier for segmentation
    CASE
        WHEN price = 0              THEN 'Free'
        WHEN price <= 50            THEN 'Budget (0-50)'
        WHEN price <= 200           THEN 'Mid-range (50-200)'
        WHEN price <= 797.83        THEN 'Premium (200-798)'
        ELSE                             'High-value (798+)'
    END                             AS price_tier,

    -- Derived day name for readability in Tableau
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END                             AS day_name,

    -- Derived month name for Tableau labels
    CASE event_month
        WHEN '2019-10-01' THEN 'October 2019'
        WHEN '2019-11-01' THEN 'November 2019'
        ELSE 'Other'
    END                             AS month_name

FROM raw_events;


-- ==============================================
-- SECTION 2: verify the view
-- Confirms row count matches raw_events exactly
-- ==============================================

SELECT COUNT(*) AS view_row_count
FROM vw_clean_events;


-- ==============================================
-- SECTION 3: verify derived columns
-- Check a sample to confirm funnel_stage,
-- price_tier and day_name are populated
-- ==============================================

SELECT
    event_type,
    funnel_stage,
    price,
    price_tier,
    day_of_week,
    day_name,
    event_week,
    month_name
FROM vw_clean_events
LIMIT 20;


-- ==============================================
-- SECTION 4: funnel stage summary
-- First proper look at the clean funnel shape
-- ==============================================

SELECT
    funnel_stage,
    COUNT(*)                            AS total_events,
    COUNT(DISTINCT user_id)             AS unique_users,
    ROUND(AVG(price), 2)                AS avg_price,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)       AS pct_of_total
FROM vw_clean_events
GROUP BY funnel_stage
ORDER BY total_events DESC;


-- ==============================================
-- SECTION 5: price tier distribution
-- Shows revenue potential across price segments
-- ==============================================

SELECT
    price_tier,
    COUNT(*)                            AS total_events,
    COUNT(DISTINCT user_id)             AS unique_users,
    COUNT(*) FILTER (
        WHERE event_type = 'purchase')  AS purchases,
    ROUND(AVG(price), 2)                AS avg_price
FROM vw_clean_events
GROUP BY price_tier
ORDER BY avg_price;


-- ==============================================
-- SECTION 6: weekly funnel summary
-- Preview of the week over week analysis
-- ==============================================

SELECT
    event_week,
    month_name,
    COUNT(*) FILTER (
        WHERE event_type = 'view')      AS views,
    COUNT(*) FILTER (
        WHERE event_type = 'cart')      AS carts,
    COUNT(*) FILTER (
        WHERE event_type = 'purchase')  AS purchases,
    COUNT(DISTINCT user_id)             AS unique_users
FROM vw_clean_events
GROUP BY event_week, month_name
ORDER BY event_week;


-- ==============================================
-- SECTION 7: master clean summary
-- Final preprocessing documentation
-- ==============================================

SELECT
    'vw_clean_events created'       AS action,
    109819981                       AS rows,
    'View over raw_events'          AS type
UNION ALL
SELECT
    'funnel_stage derived'          AS action,
    109819981                       AS rows,
    '3 stages mapped'               AS type
UNION ALL
SELECT
    'price_tier derived'            AS action,
    109819981                       AS rows,
    '5 tiers: Free to High-value'   AS type
UNION ALL
SELECT
    'day_name derived'              AS action,
    109819981                       AS rows,
    'Sunday through Saturday'       AS type
UNION ALL
SELECT
    'month_name derived'            AS action,
    109819981                       AS rows,
    'October and November 2019'     AS type;