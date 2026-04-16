-- ==============================================
-- 13_final_summary.sql
-- Executive summary of all funnel metrics.
-- This is the single source of truth for the
-- dashboard KPI tiles and narrative section.
--
-- Pulls key metrics from all previous analysis
-- scripts into one consolidated report.
-- ==============================================


-- ==============================================
-- SECTION 1: dataset overview
-- High level data quality and scale summary
-- ==============================================

SELECT
    'Raw rows loaded'               AS metric,
    '109,950,743'                   AS value,
    'Oct + Nov 2019 combined'       AS context
UNION ALL
SELECT
    'Rows after cleaning',
    '109,819,981',
    '130,762 removed in preprocessing'
UNION ALL
SELECT
    'Nulls handled',
    '50,745,035',
    'category_code + brand filled'
UNION ALL
SELECT
    'Duplicates removed',
    '130,750',
    'Cart flooding + page refreshes'
UNION ALL
SELECT
    'Outliers flagged',
    '9,874,309',
    'Price > 797.83 or = 0'
UNION ALL
SELECT
    'Date range',
    '62 days',
    '2019-10-01 to 2019-11-30'
UNION ALL
SELECT
    'Unique users',
    '5,316,649',
    'Across both months'
UNION ALL
SELECT
    'Unique products',
    '206,876',
    'Across 130 categories'
UNION ALL
SELECT
    'Unique brands',
    '4,304',
    'After unknown_brand fill';


-- ==============================================
-- SECTION 2: funnel performance summary
-- The 7 stage funnel in one table
-- ==============================================

SELECT
    stage,
    stage_name,
    users,
    conversion_rate_pct,
    dropoff_rate_pct,
    dropoff_users
FROM (
    VALUES
        (1, 'Exposure',     5076814, NULL,  NULL,    NULL),
        (2, 'View',         5076318, 99.99, 0.01,     496),
        (3, 'Repeat view',  3225600, 63.54, 36.46, 1850718),
        (4, 'Cart',          980376, 19.31, 80.69, 4095942),
        (5, 'Cart abandon',  454687, 46.38,  NULL,    NULL),
        (6, 'Purchase',      655222, 66.83, 33.17,  325154),
        (7, 'Repeat buy',    270374, 41.26, 58.74,  384848)
) AS t(stage, stage_name, users,
       conversion_rate_pct, dropoff_rate_pct,
       dropoff_users)
ORDER BY stage;


-- ==============================================
-- SECTION 3: top 5 KPI tiles
-- These feed directly into Tableau KPI cards
-- ==============================================

SELECT
    'Overall conversion rate'       AS kpi,
    '12.90%'                        AS value,
    'View to purchase'              AS definition
UNION ALL
SELECT
    'Biggest drop-off stage',
    'View to Cart — 80.69%',
    '4,095,942 users lost'
UNION ALL
SELECT
    'Total revenue (9 weeks)',
    '$315,077,167',
    'Excl. price outliers'
UNION ALL
SELECT
    'Best converting category',
    'electronics.smartphone',
    '12.99% overall conversion'
UNION ALL
SELECT
    'Singles Day revenue share',
    '21.53%',
    'Nov 11 week alone';


-- ==============================================
-- SECTION 4: revenue summary
-- ==============================================

SELECT
    'Total revenue'                 AS metric,
    '$315,077,167'                  AS value,
    '9 weeks Oct-Nov 2019'          AS context
UNION ALL
SELECT
    'Premium tier revenue share',
    '44.80%',
    '$226M from $200-798 products'
UNION ALL
SELECT
    'High-value tier revenue share',
    '37.62%',
    '$190M from $798+ products'
UNION ALL
SELECT
    'Top 10% users revenue share',
    '49.83%',
    '65,524 users drive half of revenue'
UNION ALL
SELECT
    'Apple revenue per viewer',
    '$83.56',
    'Most efficient brand'
UNION ALL
SELECT
    'Smartphone revenue per viewer',
    '$80.28',
    'Most efficient category'
UNION ALL
SELECT
    'Singles Day week revenue',
    '$67,841,094',
    '116.94% WoW increase';


-- ==============================================
-- SECTION 5: top business recommendations
-- The actionable insights for the dashboard
-- narrative footer
-- ==============================================

SELECT
    1                               AS priority,
    'Fix the view to cart gap'      AS recommendation,
    '80.69% of viewers never cart — ' ||
    'improve product discovery, ' ||
    'filtering and recommendations' AS rationale,
    '4,095,942 users'               AS opportunity
UNION ALL
SELECT
    2,
    'Recover cart abandoners',
    '454,687 users carted but never ' ||
    'purchased. Price is NOT the ' ||
    'barrier — avg cart price is ' ||
    'identical between converters ' ||
    'and abandoners ($209 vs $208). ' ||
    'Focus on re-engagement emails ' ||
    'and session continuity',
    '$94M potential revenue'
UNION ALL
SELECT
    3,
    'Invest in weekend campaigns',
    'Sunday converts at 13.26% vs ' ||
    'Friday at 7.61%. Shift ad ' ||
    'spend toward weekend slots',
    '70% higher Sunday conversion'
UNION ALL
SELECT
    4,
    'Protect top 10% of buyers',
    'Top 10% generate 49.83% of ' ||
    'revenue with avg spend of ' ||
    '$2,396 and 8.2 purchases each. ' ||
    'Loyalty programme ROI is ' ||
    'exceptionally high for this group',
    '65,524 users, $157M revenue'
UNION ALL
SELECT
    5,
    'Feature Apple products prominently',
    'Apple generates $83.56 revenue ' ||
    'per viewer vs Samsung $49.42. ' ||
    'Apple browsers are 69% more ' ||
    'valuable per visit',
    '$83.56 vs $49.42 per viewer'
ORDER BY priority;


-- ==============================================
-- SECTION 6: preprocessing impact summary
-- Documents the value of data cleaning
-- for the dashboard narrative
-- ==============================================

SELECT
    'Before cleaning'               AS state,
    '109,950,743'                   AS total_rows,
    '35,413,780'                    AS null_category,
    '15,331,243'                    AS null_brand,
    '130,750'                       AS duplicates,
    '9,874,309'                     AS outliers_flagged
UNION ALL
SELECT
    'After cleaning',
    '109,819,981',
    '0',
    '0',
    '0',
    '9,874,309';


-- ==============================================
-- SECTION 7: analytical decisions summary
-- Key methodological choices documented
-- ==============================================

SELECT
    1   AS decision_num,
    'IQR outlier detection'         AS decision,
    'Flag not delete — 163,771 ' ||
    'legitimate high value purchases ' ||
    'above the fence'               AS rationale
UNION ALL
SELECT
    2,
    'Fill nulls not delete',
    'Dropping 32% of rows for null ' ||
    'category_code would destroy ' ||
    'funnel statistical validity'
UNION ALL
SELECT
    3,
    'Window functions over subqueries',
    'IN (SELECT) ran 48+ mins on ' ||
    '109M rows — CTEs completed ' ||
    'in 6 mins'
UNION ALL
SELECT
    4,
    'Flag duplicates not delete view first',
    'Identified cart flooding as ' ||
    'frontend bug — 78 identical ' ||
    'cart events in one session'
UNION ALL
SELECT
    5,
    'Keep Singles Day spike',
    'Real user behaviour — removing ' ||
    'it would be dishonest and lose ' ||
    'the most important business insight'
ORDER BY decision_num;