-- ==============================================
-- 08_funnel_conversion.sql
-- Calculates conversion rates between each
-- stage of the 7-stage funnel.
--
-- Funnel stages derived from raw event types:
--   Stage 1 — Exposure    (all unique users)
--   Stage 2 — View        (viewed at least 1 product)
--   Stage 3 — Repeat view (viewed same product 2+)
--   Stage 4 — Cart        (added at least 1 to cart)
--   Stage 5 — Cart abandon(carted, never purchased)
--   Stage 6 — Purchase    (completed 1+ purchase)
--   Stage 7 — Repeat buy  (completed 2+ purchases)
--
-- All queries exclude price outliers and zero
-- price rows for clean conversion metrics.
-- ==============================================


-- ==============================================
-- SECTION 1: core funnel user counts
-- Rewritten using window functions for
-- performance on 109M rows.
-- Pre-aggregates user behaviour in one pass
-- rather than using nested IN (SELECT) lookups
-- ==============================================

WITH user_behaviour AS (
    SELECT
        user_id,
        -- flags per user in a single scan
        MAX(CASE WHEN event_type = 'view'
            THEN 1 ELSE 0 END)              AS did_view,
        MAX(CASE WHEN event_type = 'cart'
            THEN 1 ELSE 0 END)              AS did_cart,
        MAX(CASE WHEN event_type = 'purchase'
            THEN 1 ELSE 0 END)              AS did_purchase,
        COUNT(CASE WHEN event_type = 'purchase'
            THEN 1 END)                     AS purchase_count
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY user_id
),
repeat_viewers AS (
    SELECT DISTINCT user_id
    FROM vw_clean_events
    WHERE event_type = 'view'
      AND is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY user_id, product_id
    HAVING COUNT(*) >= 2
),
stage_counts AS (
    SELECT
        COUNT(*)                            AS s1_exposure,
        SUM(did_view)                       AS s2_view,
        COUNT(DISTINCT rv.user_id)          AS s3_repeat_view,
        SUM(did_cart)                       AS s4_cart,
        SUM(CASE WHEN did_cart = 1
            AND did_purchase = 0
            THEN 1 ELSE 0 END)              AS s5_cart_abandon,
        SUM(did_purchase)                   AS s6_purchase,
        SUM(CASE WHEN purchase_count >= 2
            THEN 1 ELSE 0 END)              AS s7_repeat_buy
    FROM user_behaviour ub
    LEFT JOIN repeat_viewers rv
        ON ub.user_id = rv.user_id
)
SELECT
    'Stage 1'           AS stage,
    'Exposure'          AS stage_name,
    s1_exposure         AS users,
    NULL                AS prev_stage_users,
    NULL                AS conversion_rate_pct,
    NULL                AS dropoff_rate_pct,
    NULL                AS dropoff_users
FROM stage_counts

UNION ALL

SELECT
    'Stage 2',
    'View',
    s2_view,
    s1_exposure,
    ROUND(s2_view * 100.0 / s1_exposure, 2),
    ROUND((s1_exposure - s2_view) * 100.0 /
        s1_exposure, 2),
    s1_exposure - s2_view
FROM stage_counts

UNION ALL

SELECT
    'Stage 3',
    'Repeat view',
    s3_repeat_view,
    s2_view,
    ROUND(s3_repeat_view * 100.0 / s2_view, 2),
    ROUND((s2_view - s3_repeat_view) * 100.0 /
        s2_view, 2),
    s2_view - s3_repeat_view
FROM stage_counts

UNION ALL

SELECT
    'Stage 4',
    'Cart',
    s4_cart,
    s2_view,
    ROUND(s4_cart * 100.0 / s2_view, 2),
    ROUND((s2_view - s4_cart) * 100.0 /
        s2_view, 2),
    s2_view - s4_cart
FROM stage_counts

UNION ALL

SELECT
    'Stage 5',
    'Cart abandon',
    s5_cart_abandon,
    s4_cart,
    ROUND(s5_cart_abandon * 100.0 / s4_cart, 2),
    NULL,
    NULL
FROM stage_counts

UNION ALL

SELECT
    'Stage 6',
    'Purchase',
    s6_purchase,
    s4_cart,
    ROUND(s6_purchase * 100.0 / s4_cart, 2),
    ROUND((s4_cart - s6_purchase) * 100.0 /
        s4_cart, 2),
    s4_cart - s6_purchase
FROM stage_counts

UNION ALL

SELECT
    'Stage 7',
    'Repeat buy',
    s7_repeat_buy,
    s6_purchase,
    ROUND(s7_repeat_buy * 100.0 / s6_purchase, 2),
    ROUND((s6_purchase - s7_repeat_buy) * 100.0 /
        s6_purchase, 2),
    s6_purchase - s7_repeat_buy
FROM stage_counts

ORDER BY stage;

-- ==============================================
-- SECTION 2: overall funnel conversion summary
-- Single row summary of top level metrics
-- ==============================================

WITH purchases AS (
    SELECT user_id, COUNT(*) AS purchase_count
    FROM vw_clean_events
    WHERE event_type = 'purchase'
      AND is_price_outlier = FALSE
      AND is_zero_price = FALSE
    GROUP BY user_id
)
SELECT
    COUNT(DISTINCT v.user_id)               AS total_users,
    COUNT(DISTINCT c.user_id)               AS users_who_carted,
    COUNT(DISTINCT p.user_id)               AS users_who_purchased,
    ROUND(COUNT(DISTINCT c.user_id) * 100.0
        / COUNT(DISTINCT v.user_id), 2)     AS view_to_cart_pct,
    ROUND(COUNT(DISTINCT p.user_id) * 100.0
        / COUNT(DISTINCT c.user_id), 2)     AS cart_to_purchase_pct,
    ROUND(COUNT(DISTINCT p.user_id) * 100.0
        / COUNT(DISTINCT v.user_id), 2)     AS overall_conversion_pct
FROM vw_clean_events v
LEFT JOIN vw_clean_events c
    ON v.user_id = c.user_id
    AND c.event_type = 'cart'
LEFT JOIN purchases p
    ON v.user_id = p.user_id
WHERE v.event_type = 'view'
  AND v.is_price_outlier = FALSE
  AND v.is_zero_price = FALSE;


-- ==============================================
-- SECTION 3: conversion by price tier
-- Shows which price segments convert best
-- ==============================================

SELECT
    price_tier,
    COUNT(DISTINCT user_id)                 AS unique_users,
    COUNT(DISTINCT CASE WHEN event_type =
        'cart' THEN user_id END)            AS carted_users,
    COUNT(DISTINCT CASE WHEN event_type =
        'purchase' THEN user_id END)        AS purchased_users,
    ROUND(COUNT(DISTINCT CASE WHEN
        event_type = 'cart'
        THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS cart_rate_pct,
    ROUND(COUNT(DISTINCT CASE WHEN
        event_type = 'purchase'
        THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS purchase_rate_pct
FROM vw_clean_events
WHERE is_price_outlier = FALSE
  AND is_zero_price    = FALSE
GROUP BY price_tier
ORDER BY purchase_rate_pct DESC;


-- ==============================================
-- SECTION 4: conversion by day of week
-- Shows which days drive highest conversion
-- ==============================================

SELECT
    day_name,
    day_of_week,
    COUNT(DISTINCT user_id)                 AS unique_users,
    COUNT(DISTINCT CASE WHEN event_type =
        'purchase' THEN user_id END)        AS purchasers,
    ROUND(COUNT(DISTINCT CASE WHEN
        event_type = 'purchase'
        THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS purchase_rate_pct
FROM vw_clean_events
WHERE is_price_outlier = FALSE
  AND is_zero_price    = FALSE
GROUP BY day_name, day_of_week
ORDER BY day_of_week;