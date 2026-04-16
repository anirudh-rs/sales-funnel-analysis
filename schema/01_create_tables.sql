-- ==============================================
-- 01_create_tables.sql
-- Creates the raw events table for both monthly
-- files and the three synthetic support tables
-- ==============================================

-- Drop tables if they already exist (safe to re-run)
DROP TABLE IF EXISTS raw_events;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS marketing_spend;

-- -----------------------------------------------
-- Table 1: raw_events
-- Holds the combined Oct + Nov Kaggle data
-- -----------------------------------------------
CREATE TABLE raw_events (
    event_time      TIMESTAMPTZ,
    event_type      VARCHAR(20),
    product_id      BIGINT,
    category_id     BIGINT,
    category_code   VARCHAR(100),
    brand           VARCHAR(100),
    price           NUMERIC(10,2),
    user_id         BIGINT,
    user_session    VARCHAR(50)
);

-- -----------------------------------------------
-- Table 2: users
-- Synthetic user demographics table
-- Deliberately contains nulls + inconsistencies
-- -----------------------------------------------
CREATE TABLE users (
    user_id         BIGINT PRIMARY KEY,
    country         VARCHAR(50),
    age_group       VARCHAR(20),
    gender          VARCHAR(10),
    signup_date     VARCHAR(30),    -- stored as VARCHAR intentionally
    loyalty_tier    VARCHAR(20)     -- some values will be missing
);

-- -----------------------------------------------
-- Table 3: sessions
-- Synthetic session metadata
-- Contains outlier durations + mixed source labels
-- -----------------------------------------------
CREATE TABLE sessions (
    user_session    VARCHAR(50) PRIMARY KEY,
    user_id         BIGINT,
    traffic_source  VARCHAR(50),
    device_type     VARCHAR(20),
    session_duration_sec  INTEGER,  -- contains outliers (bots)
    referrer_url    VARCHAR(200)
);

-- -----------------------------------------------
-- Table 4: marketing_spend
-- Synthetic weekly ad spend per channel
-- Contains missing weeks + null spend values
-- -----------------------------------------------
CREATE TABLE marketing_spend (
    week_start      DATE,
    channel         VARCHAR(50),
    spend_usd       NUMERIC(10,2),  -- some weeks null
    impressions     INTEGER,
    clicks          INTEGER
);

-- -----------------------------------------------
-- Verify all 4 tables were created
-- -----------------------------------------------
SELECT
    table_name,
    COUNT(*) OVER () AS total_tables
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;