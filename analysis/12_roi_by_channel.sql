-- ==============================================
-- 12_roi_by_channel.sql
-- Revenue and ROI analysis by price tier,
-- category and brand.
--
-- Note: This dataset has no marketing spend
-- column. ROI is measured as revenue efficiency
-- — revenue generated per user exposed,
-- rather than return on ad spend.
--
-- Total revenue across full period:
--   $315,077,167.05 across 9 weeks
--   Singles Day week = 21.53% of total revenue
--
-- Analysis angles:
--   1. Revenue by price tier
--   2. Revenue efficiency by category
--   3. Revenue efficiency by brand
--   4. High value user profile
--   5. Revenue concentration analysis
-- ==============================================


-- ==============================================
-- SECTION 1: revenue by price tier
-- Shows which price segments drive most revenue
-- ==============================================

SELECT
    price_tier,
    COUNT(CASE WHEN event_type =
        'purchase' THEN 1 END)              AS total_purchases,
    COUNT(DISTINCT CASE WHEN event_type =
        'purchase' THEN user_id END)        AS unique_buyers,
    ROUND(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END), 2)                     AS total_revenue,
    ROUND(AVG(CASE WHEN event_type =
        'purchase' THEN price END), 2)      AS avg_purchase_price,
    ROUND(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END) * 100.0 /
        SUM(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END)) OVER (), 2)            AS pct_of_total_revenue,
    ROUND(SUM(CASE WHEN event_type =
        'purchase' THEN price
        ELSE 0 END) /
        NULLIF(COUNT(DISTINCT user_id),
        0), 2)                              AS revenue_per_user
FROM vw_clean_events
WHERE is_zero_price = FALSE
GROUP BY price_tier
ORDER BY total_revenue DESC;


-- ==============================================
-- SECTION 2: revenue efficiency by category
-- Revenue per viewer — the true ROI metric
-- for a dataset without ad spend data
-- ==============================================

WITH category_revenue AS (
    SELECT
        category_code,
        COUNT(DISTINCT user_id)             AS total_users,
        COUNT(DISTINCT CASE WHEN event_type =
            'view' THEN user_id END)        AS viewers,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)          AS purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                 AS total_revenue,
        ROUND(AVG(CASE WHEN event_type =
            'purchase' THEN price
            END), 2)                        AS avg_price
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND category_code   != 'uncategorised'
    GROUP BY category_code
    HAVING COUNT(DISTINCT CASE WHEN
        event_type = 'view'
        THEN user_id END) >= 1000
)
SELECT
    category_code,
    viewers,
    purchases,
    total_revenue,
    avg_price,
    ROUND(total_revenue /
        NULLIF(viewers, 0), 2)              AS revenue_per_viewer,
    ROUND(total_revenue /
        NULLIF(purchases, 0), 2)            AS revenue_per_purchase,
    RANK() OVER (ORDER BY
        total_revenue /
        NULLIF(viewers, 0) DESC)            AS efficiency_rank
FROM category_revenue
ORDER BY revenue_per_viewer DESC
LIMIT 15;


-- ==============================================
-- SECTION 3: revenue efficiency by brand
-- ==============================================

WITH brand_revenue AS (
    SELECT
        brand,
        COUNT(DISTINCT user_id)             AS total_users,
        COUNT(DISTINCT CASE WHEN event_type =
            'view' THEN user_id END)        AS viewers,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)          AS purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                 AS total_revenue,
        ROUND(AVG(CASE WHEN event_type =
            'purchase' THEN price
            END), 2)                        AS avg_price
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND brand            != 'unknown_brand'
    GROUP BY brand
    HAVING COUNT(DISTINCT CASE WHEN
        event_type = 'view'
        THEN user_id END) >= 1000
)
SELECT
    brand,
    viewers,
    purchases,
    total_revenue,
    avg_price,
    ROUND(total_revenue /
        NULLIF(viewers, 0), 2)              AS revenue_per_viewer,
    ROUND(total_revenue /
        NULLIF(purchases, 0), 2)            AS revenue_per_purchase,
    RANK() OVER (ORDER BY
        total_revenue /
        NULLIF(viewers, 0) DESC)            AS efficiency_rank
FROM brand_revenue
ORDER BY revenue_per_viewer DESC
LIMIT 15;


-- ==============================================
-- SECTION 4: high value user profile
-- Segments users by total spend and compares
-- behaviour between segments
-- ==============================================

WITH user_spend AS (
    SELECT
        user_id,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)          AS purchase_count,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                 AS total_spend,
        COUNT(CASE WHEN event_type =
            'view' THEN 1 END)              AS view_count,
        COUNT(CASE WHEN event_type =
            'cart' THEN 1 END)              AS cart_count,
        COUNT(DISTINCT CASE WHEN event_type
            IN ('view','cart','purchase')
            THEN category_code END)         AS categories_browsed
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY user_id
    HAVING COUNT(CASE WHEN event_type =
        'purchase' THEN 1 END) > 0
),
spend_percentiles AS (
    SELECT
        PERCENTILE_CONT(0.75)
            WITHIN GROUP (
            ORDER BY total_spend)           AS p75_spend,
        PERCENTILE_CONT(0.90)
            WITHIN GROUP (
            ORDER BY total_spend)           AS p90_spend
    FROM user_spend
)
SELECT
    CASE
        WHEN us.total_spend >= sp.p90_spend
            THEN 'Top 10% — high value'
        WHEN us.total_spend >= sp.p75_spend
            THEN 'Top 25% — mid value'
        ELSE 'Bottom 75% — low value'
    END                                     AS user_segment,
    COUNT(*)                                AS users,
    ROUND(AVG(us.total_spend), 2)           AS avg_spend,
    ROUND(SUM(us.total_spend), 2)           AS total_spend,
    ROUND(SUM(us.total_spend) * 100.0 /
        SUM(SUM(us.total_spend))
        OVER (), 2)                         AS pct_of_revenue,
    ROUND(AVG(us.purchase_count), 1)        AS avg_purchases,
    ROUND(AVG(us.view_count), 0)            AS avg_views,
    ROUND(AVG(us.categories_browsed), 1)    AS avg_categories
FROM user_spend us
CROSS JOIN spend_percentiles sp
GROUP BY
    CASE
        WHEN us.total_spend >= sp.p90_spend
            THEN 'Top 10% — high value'
        WHEN us.total_spend >= sp.p75_spend
            THEN 'Top 25% — mid value'
        ELSE 'Bottom 75% — low value'
    END
ORDER BY avg_spend DESC;


-- ==============================================
-- SECTION 5: revenue concentration
-- How concentrated is revenue across users
-- Pareto analysis — do 20% of users drive 80%
-- of revenue?
-- ==============================================

WITH user_revenue AS (
    SELECT
        user_id,
        ROUND(SUM(price), 2)                AS total_spend
    FROM vw_clean_events
    WHERE event_type    = 'purchase'
      AND is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY user_id
),
ranked AS (
    SELECT
        user_id,
        total_spend,
        NTILE(10) OVER (
            ORDER BY total_spend DESC)      AS decile
    FROM user_revenue
)
SELECT
    decile,
    COUNT(*)                                AS users,
    ROUND(SUM(total_spend), 2)              AS decile_revenue,
    ROUND(SUM(total_spend) * 100.0 /
        SUM(SUM(total_spend))
        OVER (), 2)                         AS pct_of_revenue,
    ROUND(SUM(SUM(total_spend))
        OVER (ORDER BY decile
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW) * 100.0 /
        SUM(SUM(total_spend))
        OVER (), 2)                         AS cumulative_pct
FROM ranked
GROUP BY decile
ORDER BY decile;