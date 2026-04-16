-- ==============================================
-- 11_week_over_week.sql
-- Week over week funnel performance tracking
-- using LAG/LEAD window functions.
--
-- Key findings from previous analysis:
--   Nov 11 spike — Singles Day / Black Friday
--   Cart abandon rate varies 41-115% week on week
--   Cross week attribution causes >100% conv rates
--   in early weeks
--
-- Techniques used:
--   LAG()  — compare to previous week
--   LEAD() — preview next week
--   ROUND() — clean percentage formatting
--   DATE_TRUNC — weekly bucketing
-- ==============================================


-- ==============================================
-- SECTION 1: core week over week metrics
-- Purchase count, revenue and conversion
-- with LAG comparison to previous week
-- ==============================================

WITH weekly_metrics AS (
    SELECT
        event_week,
        COUNT(DISTINCT user_id)                 AS unique_users,
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
    GROUP BY event_week
)
SELECT
    event_week,
    unique_users,
    viewers,
    carters,
    purchasers,
    total_purchases,
    total_revenue,
    avg_purchase_price,

    -- Week over week changes using LAG
    LAG(purchasers)
        OVER (ORDER BY event_week)              AS prev_week_purchasers,
    purchasers - LAG(purchasers)
        OVER (ORDER BY event_week)              AS purchaser_change,
    ROUND((purchasers - LAG(purchasers)
        OVER (ORDER BY event_week)) * 100.0 /
        NULLIF(LAG(purchasers)
        OVER (ORDER BY event_week), 0), 2)      AS purchaser_wow_pct,

    LAG(total_revenue)
        OVER (ORDER BY event_week)              AS prev_week_revenue,
    ROUND(total_revenue - LAG(total_revenue)
        OVER (ORDER BY event_week), 2)          AS revenue_change,
    ROUND((total_revenue - LAG(total_revenue)
        OVER (ORDER BY event_week)) * 100.0 /
        NULLIF(LAG(total_revenue)
        OVER (ORDER BY event_week), 0), 2)      AS revenue_wow_pct,

    -- Next week preview using LEAD
    LEAD(purchasers)
        OVER (ORDER BY event_week)              AS next_week_purchasers,
    LEAD(total_revenue)
        OVER (ORDER BY event_week)              AS next_week_revenue
FROM weekly_metrics
ORDER BY event_week;


-- ==============================================
-- SECTION 2: rolling 3 week averages
-- Smooths out the Singles Day spike to show
-- underlying trend more clearly
-- ==============================================

WITH weekly_metrics AS (
    SELECT
        event_week,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)              AS total_purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                     AS total_revenue,
        COUNT(DISTINCT CASE WHEN event_type =
            'cart' THEN user_id END)            AS carters,
        COUNT(DISTINCT CASE WHEN event_type =
            'purchase' THEN user_id END)        AS purchasers
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY event_week
)
SELECT
    event_week,
    total_purchases,
    total_revenue,
    ROUND(AVG(total_purchases)
        OVER (ORDER BY event_week
        ROWS BETWEEN 2 PRECEDING
        AND CURRENT ROW), 0)                    AS rolling_3w_purchases,
    ROUND(AVG(total_revenue)
        OVER (ORDER BY event_week
        ROWS BETWEEN 2 PRECEDING
        AND CURRENT ROW), 2)                    AS rolling_3w_revenue,
    ROUND(AVG(ROUND(purchasers * 100.0 /
        NULLIF(carters, 0), 2))
        OVER (ORDER BY event_week
        ROWS BETWEEN 2 PRECEDING
        AND CURRENT ROW), 2)                    AS rolling_3w_conv_pct
FROM weekly_metrics
ORDER BY event_week;


-- ==============================================
-- SECTION 3: week over week by category
-- Top 5 categories tracked week by week
-- Shows which categories drove the Nov 11 spike
-- ==============================================

WITH weekly_category AS (
    SELECT
        event_week,
        category_code,
        COUNT(CASE WHEN event_type =
            'purchase' THEN 1 END)              AS purchases,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                     AS revenue
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
      AND category_code IN (
            'electronics.smartphone',
            'electronics.video.tv',
            'computers.notebook',
            'electronics.clocks',
            'electronics.audio.headphone')
    GROUP BY event_week, category_code
)
SELECT
    event_week,
    category_code,
    purchases,
    revenue,
    LAG(purchases)
        OVER (PARTITION BY category_code
        ORDER BY event_week)                    AS prev_week_purchases,
    ROUND((purchases - LAG(purchases)
        OVER (PARTITION BY category_code
        ORDER BY event_week)) * 100.0 /
        NULLIF(LAG(purchases)
        OVER (PARTITION BY category_code
        ORDER BY event_week), 0), 2)            AS wow_pct_change
FROM weekly_category
ORDER BY category_code, event_week;


-- ==============================================
-- SECTION 4: cumulative revenue by week
-- Running total of revenue across the period
-- ==============================================

WITH weekly_revenue AS (
    SELECT
        event_week,
        ROUND(SUM(CASE WHEN event_type =
            'purchase' THEN price
            ELSE 0 END), 2)                     AS weekly_revenue
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY event_week
)
SELECT
    event_week,
    weekly_revenue,
    ROUND(SUM(weekly_revenue)
        OVER (ORDER BY event_week
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW), 2)                    AS cumulative_revenue,
    ROUND(weekly_revenue * 100.0 /
        SUM(weekly_revenue) OVER (), 2)         AS pct_of_total_revenue
FROM weekly_revenue
ORDER BY event_week;


-- ==============================================
-- SECTION 5: week over week cart abandonment
-- Tracks whether abandonment is improving
-- or worsening each week
-- ==============================================

WITH weekly_abandon AS (
    SELECT
        event_week,
        COUNT(DISTINCT CASE WHEN event_type =
            'cart' THEN user_id END)            AS carters,
        COUNT(DISTINCT CASE WHEN event_type =
            'purchase' THEN user_id END)        AS purchasers
    FROM vw_clean_events
    WHERE is_price_outlier = FALSE
      AND is_zero_price    = FALSE
    GROUP BY event_week
)
SELECT
    event_week,
    carters,
    purchasers,
    carters - purchasers                        AS abandoned_users,
    ROUND((carters - purchasers) * 100.0 /
        NULLIF(carters, 0), 2)                  AS abandon_rate_pct,
    LAG(ROUND((carters - purchasers) * 100.0 /
        NULLIF(carters, 0), 2))
        OVER (ORDER BY event_week)              AS prev_week_abandon_pct,
    ROUND((carters - purchasers) * 100.0 /
        NULLIF(carters, 0), 2) -
    LAG(ROUND((carters - purchasers) * 100.0 /
        NULLIF(carters, 0), 2))
        OVER (ORDER BY event_week)              AS abandon_rate_change
FROM weekly_abandon
ORDER BY event_week;