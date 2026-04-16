-- ==============================================
-- 09_dropoff_analysis.sql
-- Identifies where and why users abandon the
-- funnel at each stage.
--
-- Builds on funnel_conversion.sql findings:
--   Biggest drop: View → Cart (80.69%)
--   Cart abandon rate: 46.38%
--   Overall conversion: 12.90%
--
-- Analysis angles:
--   1. Drop-off by funnel stage
--   2. Drop-off by price tier
--   3. Drop-off by day of week
--   4. Drop-off by category
--   5. Drop-off by brand (top 20)
--   6. Cart abandonment deep dive
-- ==============================================


-- ==============================================
-- SECTION 1: drop-off volume and rate by stage
-- The core drop-off table for Tableau waterfall
-- ==============================================

WITH user_behaviour AS (
    SELECT
        user_id,
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
)
SELECT
    stage,
    stage_name,
    users,
    dropped_at_stage,
    ROUND(dropped_at_stage * 100.0 /
        NULLIF(users, 0), 2)                AS dropoff_rate_pct,
    ROUND(users * 100.0 /
        MAX(users) OVER (), 2)              AS pct_of_total_users
FROM (
    SELECT
        1                                   AS stage,
        'Exposure'                          AS stage_name,
        COUNT(*)                            AS users,
        COUNT(*) - SUM(did_view)            AS dropped_at_stage
    FROM user_behaviour

    UNION ALL

    SELECT
        2,
        'View',
        SUM(did_view),
        SUM(did_view) - SUM(did_cart)
    FROM user_behaviour

    UNION ALL

    SELECT
        3,
        'Cart',
        SUM(did_cart),
        SUM(CASE WHEN did_cart = 1
            AND did_purchase = 0
            THEN 1 ELSE 0 END)
    FROM user_behaviour

    UNION ALL

    SELECT
        4,
        'Purchase',
        SUM(did_purchase),
        SUM(did_purchase) - SUM(CASE WHEN
            purchase_count >= 2
            THEN 1 ELSE 0 END)
    FROM user_behaviour

    UNION ALL

    SELECT
        5,
        'Repeat buy',
        SUM(CASE WHEN purchase_count >= 2
            THEN 1 ELSE 0 END),
        0
    FROM user_behaviour
) stages
ORDER BY stage;


-- ==============================================
-- SECTION 2: drop-off by category
-- Which product categories lose the most users
-- between view and purchase
-- ==============================================

WITH category_funnel AS (
    SELECT
        category_code,
        COUNT(DISTINCT CASE WHEN event_type = 'view'
            THEN user_id END)               AS viewers,
        COUNT(DISTINCT CASE WHEN event_type = 'cart'
            THEN user_id END)               AS carters,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'
            THEN user_id END)               AS purchasers
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND category_code   != 'uncategorised'
    GROUP BY category_code
    HAVING COUNT(DISTINCT CASE WHEN event_type = 'view'
        THEN user_id END) >= 1000
)
SELECT
    category_code,
    viewers,
    carters,
    purchasers,
    ROUND(carters * 100.0 /
        NULLIF(viewers, 0), 2)              AS view_to_cart_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2)              AS cart_to_purchase_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(viewers, 0), 2)              AS overall_conversion_pct,
    viewers - purchasers                    AS total_dropoff
FROM category_funnel
ORDER BY total_dropoff DESC
LIMIT 20;


-- ==============================================
-- SECTION 3: drop-off by brand (top 20)
-- Which brands lose the most users
-- ==============================================

WITH brand_funnel AS (
    SELECT
        brand,
        COUNT(DISTINCT CASE WHEN event_type = 'view'
            THEN user_id END)               AS viewers,
        COUNT(DISTINCT CASE WHEN event_type = 'cart'
            THEN user_id END)               AS carters,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'
            THEN user_id END)               AS purchasers
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND brand            != 'unknown_brand'
    GROUP BY brand
    HAVING COUNT(DISTINCT CASE WHEN event_type = 'view'
        THEN user_id END) >= 1000
)
SELECT
    brand,
    viewers,
    carters,
    purchasers,
    ROUND(carters * 100.0 /
        NULLIF(viewers, 0), 2)              AS view_to_cart_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2)              AS cart_to_purchase_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(viewers, 0), 2)              AS overall_conversion_pct
FROM brand_funnel
ORDER BY viewers DESC
LIMIT 20;


-- ==============================================
-- SECTION 4: cart abandonment deep dive
-- Profiles users who carted but never purchased
-- vs users who carted and purchased
-- ==============================================

WITH user_behaviour AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'cart'
            THEN 1 ELSE 0 END)              AS did_cart,
        MAX(CASE WHEN event_type = 'purchase'
            THEN 1 ELSE 0 END)              AS did_purchase,
        COUNT(CASE WHEN event_type = 'view'
            THEN 1 END)                     AS view_count,
        COUNT(CASE WHEN event_type = 'cart'
            THEN 1 END)                     AS cart_count,
        ROUND(AVG(CASE WHEN event_type IN
            ('cart','purchase')
            THEN price END), 2)             AS avg_cart_price,
        COUNT(DISTINCT CASE WHEN event_type
            IN ('cart','purchase')
            THEN category_code END)         AS categories_touched
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY user_id
)
SELECT
    CASE WHEN did_purchase = 1
        THEN 'Converted'
        ELSE 'Abandoned'
    END                                     AS user_segment,
    COUNT(*)                                AS users,
    ROUND(AVG(view_count), 1)               AS avg_views,
    ROUND(AVG(cart_count), 1)               AS avg_cart_events,
    ROUND(AVG(avg_cart_price), 2)           AS avg_cart_price,
    ROUND(AVG(categories_touched), 1)       AS avg_categories
FROM user_behaviour
WHERE did_cart = 1
GROUP BY did_purchase
ORDER BY did_purchase DESC;


-- ==============================================
-- SECTION 5: weekly drop-off trend
-- Shows whether drop-off is getting better
-- or worse week by week
-- ==============================================

WITH weekly_funnel AS (
    SELECT
        event_week,
        COUNT(DISTINCT CASE WHEN event_type = 'view'
            THEN user_id END)               AS viewers,
        COUNT(DISTINCT CASE WHEN event_type = 'cart'
            THEN user_id END)               AS carters,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'
            THEN user_id END)               AS purchasers
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY event_week
)
SELECT
    event_week,
    viewers,
    carters,
    purchasers,
    ROUND(carters * 100.0 /
        NULLIF(viewers, 0), 2)              AS view_to_cart_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2)              AS cart_to_purchase_pct,
    viewers - purchasers                    AS total_dropoff,
    LAG(ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2))
        OVER (ORDER BY event_week)          AS prev_week_conv_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2) -
    LAG(ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2))
        OVER (ORDER BY event_week)          AS wow_conv_change
FROM weekly_funnel
ORDER BY event_week;