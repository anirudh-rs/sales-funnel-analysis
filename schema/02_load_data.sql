-- ==============================================
-- 02_load_data.sql
-- Loads October and November event data into
-- raw_events via a single 9-column staging table.
--
-- Dataset: E-commerce Behaviour Data
-- Source:  Kaggle — mkechinov
-- Files:   events_oct.csv (8.7GB, ~42M rows)
--          events_nov.csv (5.5GB, ~67M rows)
--          Total: ~109M rows combined
--
-- Event types confirmed in data:
--   view     — 104,335,509 events (94.89%)
--   cart     —   3,955,446 events  (3.60%)
--   purchase —   1,659,788 events  (1.51%)
--
-- Derived funnel stages (built in preprocessing):
--   Stage 1 — Exposure      (all users in dataset)
--   Stage 2 — View          (viewed at least 1 product)
--   Stage 3 — Repeat view   (viewed same product 2+ times)
--   Stage 4 — Cart          (added at least 1 item to cart)
--   Stage 5 — Cart abandon  (carted but never purchased)
--   Stage 6 — Purchase      (completed at least 1 purchase)
--   Stage 7 — Repeat buy    (made more than 1 purchase)
--
-- Key behavioural findings from raw data:
--   Total unique users:       5,316,649
--   Viewed only (no cart):    4,262,367  (80.2%)
--   Cart abandoned:             488,221  (46.3% of carters)
--   Completed purchase:         697,470  (66.2% of carters)
--
-- Load order:
--   Step 1 — Run Section 1 in pgAdmin
--   Step 2 — Run Section 2 commands in CMD
--   Step 3 — Run Sections 3, 4, 5 in pgAdmin
--
-- Note: Both files are 9-column CSVs. An earlier
-- truncated version of October had 11 columns but
-- the full re-downloaded file matches November
-- exactly. Both load into the same staging table.
--
-- Warning: Do NOT open CSVs in Excel. Files exceed
-- Excel's 1,048,575 row limit and will be silently
-- truncated without any warning.
-- ==============================================


-- ==============================================
-- SECTION 1: create staging table
-- Run this in pgAdmin first
-- ==============================================

DROP TABLE IF EXISTS raw_events_stage;

CREATE TABLE raw_events_stage (
    event_time      TEXT,
    event_type      TEXT,
    product_id      TEXT,
    category_id     TEXT,
    category_code   TEXT,
    brand           TEXT,
    price           TEXT,
    user_id         TEXT,
    user_session    TEXT
);


-- ==============================================
-- SECTION 2: load CSVs via CMD
-- Run these one at a time in Command Prompt.
-- Wait for COPY xxxxxxx before running the next.
-- Replace your_username with your Windows username.
--
-- October:
-- psql -U postgres -d sales_funnel -c "\COPY raw_events_stage FROM 'C:/Users/your_username/OneDrive/Desktop/Sales Funnel/data/raw/events_oct.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '')"
--
-- November:
-- psql -U postgres -d sales_funnel -c "\COPY raw_events_stage FROM 'C:/Users/your_username/OneDrive/Desktop/Sales Funnel/data/raw/events_nov.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL '')"
--
-- Expected COPY counts:
--   October:  ~42,481,998 rows
--   November: ~67,395,246 rows
--   Staging total: ~109,950,743 rows
-- ==============================================


-- ==============================================
-- SECTION 3: verify staging before inserting
-- Run this immediately after both COPY commands.
-- Do not run anything else in between.
-- Expected: 109950743
-- ==============================================

SELECT COUNT(*) AS staging_rows FROM raw_events_stage;


-- ==============================================
-- SECTION 4: insert into raw_events
-- Only run this after confirming staging count.
-- Casts each TEXT column to its proper data type.
-- Note: category_id goes through NUMERIC first
-- before BIGINT to handle scientific notation
-- values like 2.10381E+18 in the raw data.
-- ==============================================

TRUNCATE TABLE raw_events;

INSERT INTO raw_events (
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
)
SELECT
    event_time::TIMESTAMPTZ,
    event_type,
    product_id::BIGINT,
    category_id::NUMERIC::BIGINT,
    category_code,
    brand,
    price::NUMERIC(10,2),
    user_id::BIGINT,
    user_session
FROM raw_events_stage;


-- ==============================================
-- SECTION 5: verify row counts by month
-- Run after INSERT completes
-- ==============================================

SELECT
    'October'   AS month,
    COUNT(*)    AS rows_loaded
FROM raw_events
WHERE event_time >= '2019-10-01'
  AND event_time <  '2019-11-01'

UNION ALL

SELECT
    'November'  AS month,
    COUNT(*)    AS rows_loaded
FROM raw_events
WHERE event_time >= '2019-11-01'
  AND event_time <  '2019-12-01'

UNION ALL

SELECT
    'Total'     AS month,
    COUNT(*)    AS rows_loaded
FROM raw_events;


-- ==============================================
-- SECTION 6: event type and funnel profile
-- Run after row counts are confirmed
-- Documents the raw funnel shape before
-- any preprocessing has been applied
-- ==============================================

SELECT
    event_type,
    COUNT(*)                                            AS total_events,
    COUNT(DISTINCT user_id)                             AS unique_users,
    COUNT(DISTINCT product_id)                          AS unique_products,
    ROUND(COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (), 2)                       AS pct_of_total
FROM raw_events
GROUP BY event_type
ORDER BY total_events DESC;


-- ==============================================
-- SECTION 7: derived funnel behaviour profile
-- Documents the 7-stage funnel user counts
-- before preprocessing
-- ==============================================

SELECT
    COUNT(DISTINCT user_id)                             AS total_users,

    COUNT(DISTINCT CASE WHEN event_type = 'view'
        THEN user_id END)                               AS stage_2_viewed,

    COUNT(DISTINCT CASE WHEN event_type = 'cart'
        THEN user_id END)                               AS stage_4_carted,

    COUNT(DISTINCT CASE WHEN event_type = 'purchase'
        THEN user_id END)                               AS stage_6_purchased,

    COUNT(DISTINCT CASE WHEN event_type = 'view'
        AND user_id NOT IN (
            SELECT DISTINCT user_id FROM raw_events
            WHERE event_type = 'cart')
        THEN user_id END)                               AS viewed_only,

    COUNT(DISTINCT CASE WHEN event_type = 'cart'
        AND user_id NOT IN (
            SELECT DISTINCT user_id FROM raw_events
            WHERE event_type = 'purchase')
        THEN user_id END)                               AS stage_5_cart_abandoned

FROM raw_events;


-- ==============================================
-- SECTION 8: drop staging table
-- Only run this after all verifications above
-- are confirmed correct
-- ==============================================

DROP TABLE raw_events_stage;