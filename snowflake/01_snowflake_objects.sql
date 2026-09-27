-- =============================================================================
-- 01_snowflake_objects.sql
--
-- Creates the Snowflake objects needed to load and query
-- GOLD_CUSTOMER_CATEGORY_MONTH. No credentials are embedded anywhere in
-- this file. Replace the placeholders below with your own database and
-- warehouse names before running.
-- =============================================================================

-- -----------------------------------------------------------------------
-- Placeholders -- set these once for your environment
-- -----------------------------------------------------------------------
-- <YOUR_DATABASE>   e.g. CCSS_DB
-- <YOUR_WAREHOUSE>  e.g. CCSS_WH  (an existing warehouse; not created here)

-- Schema is created here because it is specific to this project.
-- The database itself is assumed to already exist (created once by an
-- account admin); uncomment the CREATE DATABASE line only if you are
-- setting up a fresh, dedicated database for this project.

-- CREATE DATABASE IF NOT EXISTS <YOUR_DATABASE>;

CREATE SCHEMA IF NOT EXISTS <YOUR_DATABASE>.CREDIT_CARD_SPEND_SEGMENTATION;

USE SCHEMA <YOUR_DATABASE>.CREDIT_CARD_SPEND_SEGMENTATION;

-- -----------------------------------------------------------------------
-- CSV file format matching the export from 06_export_gold.py
-- -----------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS FF_GOLD_CUSTOMER_CATEGORY_MONTH
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    ENCODING = 'UTF8';

-- -----------------------------------------------------------------------
-- Named internal stage.
--
-- The exported CSV (GOLD_CUSTOMER_CATEGORY_MONTH.csv, produced by
-- 06_export_gold.py) must be uploaded to this stage through the normal
-- Snowflake upload path (SnowSQL `PUT`, Snowsight "Upload files to stage",
-- or an external stage/cloud copy step you control) BEFORE running
-- 02_snowflake_load.sql. This script does not perform that upload.
-- -----------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS STG_GOLD_CUSTOMER_CATEGORY_MONTH
    FILE_FORMAT = FF_GOLD_CUSTOMER_CATEGORY_MONTH;

-- -----------------------------------------------------------------------
-- Target table
--
-- Grain: one row per (customer_id, month, merchant_category)
-- Expected row count after load: 48,000
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS GOLD_CUSTOMER_CATEGORY_MONTH (
    customer_id            VARCHAR(16)     NOT NULL,
    city                   VARCHAR(32)     NOT NULL,
    card_type              VARCHAR(16)     NOT NULL,
    age_band               VARCHAR(16)     NOT NULL,
    month                  VARCHAR(7)      NOT NULL,   -- 'YYYY-MM'
    merchant_category      VARCHAR(32)     NOT NULL,   -- one of the 8 canonical categories
    txns                   NUMBER(10,0)    NOT NULL,   -- transaction count for this cell
    spend                  NUMBER(14,2)    NOT NULL,   -- net signed spend, refunds included
    customer_month_spend   NUMBER(14,2)    NOT NULL,   -- total spend for this customer+month
    spend_share            NUMBER(9,6)     NOT NULL,   -- spend / customer_month_spend
    is_dominant            NUMBER(1,0)     NOT NULL    -- 1 = dominant category by spend for this customer+month
);
