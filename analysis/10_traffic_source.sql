-- ==============================================
-- 10_traffic_source.sql
-- Analyses conversion performance by category
-- and brand — acting as the traffic source
-- breakdown for this dataset.
--
-- Note: This dataset does not contain a traffic
-- source column (organic/paid/social/email).
-- Category and brand serve as the primary
-- segmentation dimensions instead.
--
-- Top categories by volume:
--   electronics.smartphone   — 27,822,604 events
--   electronics.clocks       —  3,396,152 events
--   electronics.video.tv     —  3,316,458 events
--   computers.notebook       —  3,316,047 events
--   electronics.audio.headphone — 2,913,011 events
-- ==============================================


-- ==============================================
-- SECTION 1: top 15 categories by conversion
-- Full funnel metrics per category
-- ==============================================

WITH category_funnel AS (
    SELECT
        category_code,
        COUNT(DISTINCT user_id)                 AS total_users,
        COUNT(DISTINCT CASE WHEN event_type =
            'view' THEN user_id END)            AS viewers,
        COUNT(DISTINCT CASE WHEN event_type =
            'cart' THEN user_id END)            AS carters,
        COUNT(DISTINCT CASE WHEN event_type =
            'purchase' THEN user_id END)        AS purchasers,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)              AS total_purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price ELSE 0
            END), 2)                            AS total_revenue,
        ROUND(AVG(CASE WHEN event_type =
            'purchase' THEN price END), 2)      AS avg_purchase_price
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND category_code   != 'uncategorised'
    GROUP BY category_code
    HAVING COUNT(DISTINCT CASE WHEN event_type =
        'view' THEN user_id END) >= 1000
)
SELECT
    category_code,
    viewers,
    carters,
    purchasers,
    total_purchases,
    ROUND(carters * 100.0 /
        NULLIF(viewers, 0), 2)              AS view_to_cart_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2)              AS cart_to_purchase_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(viewers, 0), 2)              AS overall_conversion_pct,
    total_revenue,
    avg_purchase_price,
    ROUND(total_revenue /
        NULLIF(viewers, 0), 2)              AS revenue_per_viewer
FROM category_funnel
ORDER BY total_revenue DESC
LIMIT 15;


-- ==============================================
-- SECTION 2: category performance matrix
-- Segments categories into 4 quadrants based
-- on conversion rate vs revenue
-- High conv + high rev  = Star performers
-- High conv + low rev   = Quick wins
-- Low conv  + high rev  = Optimise checkout
-- Low conv  + low rev   = Deprioritise
-- ==============================================

WITH category_metrics AS (
    SELECT
        category_code,
        COUNT(DISTINCT CASE WHEN event_type =
            'view' THEN user_id END)            AS viewers,
        COUNT(DISTINCT CASE WHEN event_type =
            'purchase' THEN user_id END)        AS purchasers,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                     AS total_revenue,
        ROUND(COUNT(DISTINCT CASE WHEN
            event_type = 'purchase'
            THEN user_id END) * 100.0 /
            NULLIF(COUNT(DISTINCT CASE WHEN
            event_type = 'view'
            THEN user_id END), 0), 2)           AS conversion_pct
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND category_code   != 'uncategorised'
    GROUP BY category_code
    HAVING COUNT(DISTINCT CASE WHEN event_type =
        'view' THEN user_id END) >= 1000
),
averages AS (
    SELECT
        AVG(conversion_pct)                     AS avg_conv,
        AVG(total_revenue)                      AS avg_rev
    FROM category_metrics
)
SELECT
    cm.category_code,
    cm.viewers,
    cm.purchasers,
    cm.conversion_pct,
    cm.total_revenue,
    CASE
        WHEN cm.conversion_pct >= a.avg_conv
            AND cm.total_revenue >= a.avg_rev
            THEN 'Star — high conv, high rev'
        WHEN cm.conversion_pct >= a.avg_conv
            AND cm.total_revenue < a.avg_rev
            THEN 'Quick win — high conv, low rev'
        WHEN cm.conversion_pct < a.avg_conv
            AND cm.total_revenue >= a.avg_rev
            THEN 'Optimise — low conv, high rev'
        ELSE
            'Deprioritise — low conv, low rev'
    END                                         AS performance_quadrant
FROM category_metrics cm
CROSS JOIN averages a
ORDER BY cm.total_revenue DESC
LIMIT 20;


-- ==============================================
-- SECTION 3: top 15 brands by revenue
-- Full funnel metrics per brand
-- ==============================================

WITH brand_funnel AS (
    SELECT
        brand,
        COUNT(DISTINCT CASE WHEN event_type =
            'view' THEN user_id END)            AS viewers,
        COUNT(DISTINCT CASE WHEN event_type =
            'cart' THEN user_id END)            AS carters,
        COUNT(DISTINCT CASE WHEN event_type =
            'purchase' THEN user_id END)        AS purchasers,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)              AS total_purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                     AS total_revenue,
        ROUND(AVG(CASE WHEN event_type =
            'purchase' THEN price END), 2)      AS avg_purchase_price
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND brand            != 'unknown_brand'
    GROUP BY brand
    HAVING COUNT(DISTINCT CASE WHEN event_type =
        'view' THEN user_id END) >= 1000
)
SELECT
    brand,
    viewers,
    carters,
    purchasers,
    total_purchases,
    ROUND(carters * 100.0 /
        NULLIF(viewers, 0), 2)              AS view_to_cart_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2)              AS cart_to_purchase_pct,
    ROUND(purchasers * 100.0 /
        NULLIF(viewers, 0), 2)              AS overall_conversion_pct,
    total_revenue,
    avg_purchase_price,
    ROUND(total_revenue /
        NULLIF(viewers, 0), 2)              AS revenue_per_viewer
FROM brand_funnel
ORDER BY total_revenue DESC
LIMIT 15;


-- ==============================================
-- SECTION 4: hourly conversion pattern
-- Shows which hours of day convert best
-- Useful for timing promotions
-- ==============================================

SELECT
    hour_of_day,
    COUNT(DISTINCT user_id)                 AS unique_users,
    COUNT(DISTINCT CASE WHEN event_type =
        'purchase' THEN user_id END)        AS purchasers,
    COUNT(CASE WHEN event_type =
        'purchase' THEN 1 END)              AS total_purchases,
    ROUND(COUNT(DISTINCT CASE WHEN
        event_type = 'purchase'
        THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS purchase_rate_pct,
    ROUND(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END), 2)                     AS total_revenue
FROM vw_clean_events
WHERE is_price_outlier = FALSE
  AND is_zero_price    = FALSE
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- ==============================================
-- SECTION 5: weekend vs weekday performance
-- Conversion and revenue by day type
-- ==============================================

SELECT
    CASE WHEN is_weekend THEN 'Weekend'
        ELSE 'Weekday' END                  AS day_type,
    COUNT(DISTINCT user_id)                 AS unique_users,
    COUNT(DISTINCT CASE WHEN event_type =
        'purchase' THEN user_id END)        AS purchasers,
    ROUND(COUNT(DISTINCT CASE WHEN
        event_type = 'purchase'
        THEN user_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS purchase_rate_pct,
    COUNT(CASE WHEN event_type =
        'purchase' THEN 1 END)              AS total_purchases,
    ROUND(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END), 2)                     AS total_revenue,
    ROUND(AVG(CASE WHEN event_type =
        'purchase' THEN price END), 2)      AS avg_purchase_price
FROM vw_clean_events
WHERE is_price_outlier = FALSE
  AND is_zero_price    = FALSE
GROUP BY is_weekend
ORDER BY purchase_rate_pct DESC;