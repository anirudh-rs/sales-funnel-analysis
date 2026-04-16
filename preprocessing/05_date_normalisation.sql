-- ==============================================
-- 05_date_normalisation.sql
-- Normalises date and time values in raw_events
-- for consistent weekly and daily analysis.
--
-- Findings:
--   Date range: 2019-09-30 to 2019-11-30 (62 days)
--   Total weeks: 9  |  Total days: 62
--   Timezone split:
--     -04:00 (EDT): 45,700,940 rows (Oct data)
--     -05:00 (EST): 64,119,041 rows (Nov data)
--   DST change: November 3, 2019 at 02:00
--   Sep 30 spillover rows exist — flagged but kept
--
-- Strategy:
--   1. Convert all timestamps to UTC for
--      consistent arithmetic across DST boundary
--   2. Add derived date columns for analysis:
--      event_date, event_week, event_month,
--      day_of_week, hour_of_day
--   3. Flag September spillover rows
--   4. Verify week boundaries are clean
-- ==============================================


-- ==============================================
-- SECTION 1: baseline date profile
-- Documents the raw date state before changes
-- ==============================================

-- Date range and week counts
SELECT
    MIN(event_time)                             AS earliest_event,
    MAX(event_time)                             AS latest_event,
    COUNT(DISTINCT DATE_TRUNC('week',
        event_time))                            AS total_weeks,
    COUNT(DISTINCT DATE_TRUNC('day',
        event_time))                            AS total_days
FROM raw_events;

-- September spillover count
SELECT COUNT(*)                                 AS sep_spillover_rows
FROM raw_events
WHERE event_time < '2019-10-01 00:00:00+00';

-- ==============================================
-- SECTION 2: add derived date columns
-- Adds event_date, event_week, event_month,
-- day_of_week, hour_of_day to raw_events
-- All timestamps converted to UTC first
-- ==============================================

ALTER TABLE raw_events
    ADD COLUMN IF NOT EXISTS event_date     DATE,
    ADD COLUMN IF NOT EXISTS event_week     DATE,
    ADD COLUMN IF NOT EXISTS event_month    DATE,
    ADD COLUMN IF NOT EXISTS day_of_week    INTEGER,
    ADD COLUMN IF NOT EXISTS hour_of_day    INTEGER,
    ADD COLUMN IF NOT EXISTS is_weekend     BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_spillover   BOOLEAN;


-- ==============================================
-- SECTION 3: populate derived date columns
-- Converts to UTC then extracts date parts
-- This will take several minutes on 109M rows
-- ==============================================

UPDATE raw_events
SET
    event_date  = DATE(event_time AT TIME ZONE 'UTC'),
    event_week  = DATE_TRUNC('week',
                    event_time AT TIME ZONE 'UTC')::DATE,
    event_month = DATE_TRUNC('month',
                    event_time AT TIME ZONE 'UTC')::DATE,
    day_of_week = EXTRACT(DOW FROM
                    event_time AT TIME ZONE 'UTC')::INTEGER,
    hour_of_day = EXTRACT(HOUR FROM
                    event_time AT TIME ZONE 'UTC')::INTEGER,
    is_weekend  = EXTRACT(DOW FROM
                    event_time AT TIME ZONE 'UTC')
                    IN (0, 6),
    is_spillover = event_time < '2019-10-01';


-- ==============================================
-- SECTION 4: verify derived columns
-- Check a sample to confirm values look correct
-- ==============================================

SELECT
    event_time,
    event_date,
    event_week,
    event_month,
    day_of_week,
    hour_of_day,
    is_weekend,
    is_spillover
FROM raw_events
LIMIT 10;


-- ==============================================
-- SECTION 5: week distribution
-- Shows row counts per week across the dataset
-- Confirms clean weekly boundaries after
-- DST normalisation
-- ==============================================

SELECT
    event_week,
    COUNT(*)                    AS total_events,
    COUNT(DISTINCT user_id)     AS unique_users,
    COUNT(CASE WHEN event_type =
        'purchase' THEN 1 END)  AS purchases
FROM raw_events
GROUP BY event_week
ORDER BY event_week;


-- ==============================================
-- SECTION 6: DST boundary check
-- Verifies no gaps or spikes around Nov 3 2019
-- which is when US clocks moved back 1 hour
-- ==============================================

SELECT
    event_date,
    COUNT(*)                    AS total_events,
    MIN(event_time)             AS first_event,
    MAX(event_time)             AS last_event
FROM raw_events
WHERE event_date BETWEEN '2019-11-01' AND '2019-11-05'
GROUP BY event_date
ORDER BY event_date;


-- ==============================================
-- SECTION 7: date normalisation summary
-- ==============================================

SELECT
    'Derived columns added'     AS action,
    7                           AS columns_added,
    'event_date, event_week, event_month, ' ||
    'day_of_week, hour_of_day, ' ||
    'is_weekend, is_spillover'  AS detail
UNION ALL
SELECT
    'Timestamps normalised'     AS action,
    109819981                   AS rows_affected,
    'All converted to UTC'      AS detail
UNION ALL
SELECT
    'Sep spillover flagged'     AS action,
    NULL                        AS rows_affected,
    'Kept but marked for '  ||
    'optional exclusion'        AS detail;