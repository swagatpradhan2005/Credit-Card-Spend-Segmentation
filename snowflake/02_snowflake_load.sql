-- =============================================================================
-- 02_snowflake_load.sql
--
-- IMPORTANT -- MANUAL STEP FIRST:
-- Before running this script, upload the exported file
--   GOLD_CUSTOMER_CATEGORY_MONTH.csv
-- (produced by databricks/06_export_gold.py) to the stage
--   STG_GOLD_CUSTOMER_CATEGORY_MONTH
-- through the normal Snowflake upload interface/client -- e.g.:
--   PUT file:///local/path/GOLD_CUSTOMER_CATEGORY_MONTH.csv
--       @STG_GOLD_CUSTOMER_CATEGORY_MONTH;
-- or the Snowsight "Upload files to stage" UI.
-- This script does not perform that upload itself.
-- =============================================================================

USE SCHEMA <YOUR_DATABASE>.CREDIT_CARD_SPEND_SEGMENTATION;

-- -----------------------------------------------------------------------
-- Load.
--
-- Snowflake's COPY INTO tracks, per stage + file name + file checksum,
-- whether a file has already been loaded (load metadata retained for 64
-- days by default). Re-running this exact COPY INTO against the SAME
-- unchanged file will therefore load ZERO new rows the second time,
-- because we deliberately do NOT pass FORCE = TRUE.
--
-- If you intentionally regenerate and re-export a new Gold CSV later,
-- upload it under a new file name (or truncate the table first) rather
-- than reaching for FORCE = TRUE, to avoid silently reloading duplicates.
-- -----------------------------------------------------------------------
COPY INTO GOLD_CUSTOMER_CATEGORY_MONTH
FROM @STG_GOLD_CUSTOMER_CATEGORY_MONTH/GOLD_CUSTOMER_CATEGORY_MONTH.csv
FILE_FORMAT = (FORMAT_NAME = FF_GOLD_CUSTOMER_CATEGORY_MONTH)
ON_ERROR = 'ABORT_STATEMENT';

-- Run the COPY INTO above a second time (unchanged) to confirm the
-- required capstone behaviour: it should report 0 rows loaded for that
-- file on the second run, because Snowflake recognises the file was
-- already loaded and FORCE = TRUE was not used.

-- -----------------------------------------------------------------------
-- Load history -- shows exactly how many times each file was considered
-- and how many rows it contributed each time.
-- -----------------------------------------------------------------------
SELECT file_name,
       last_load_time,
       row_count,
       row_parsed,
       status
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'GOLD_CUSTOMER_CATEGORY_MONTH',
    START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- -----------------------------------------------------------------------
-- Row-count verification
-- -----------------------------------------------------------------------
SELECT COUNT(*) AS actual_row_count,
       48000    AS expected_row_count,
       COUNT(*) = 48000 AS row_count_matches_expected
FROM GOLD_CUSTOMER_CATEGORY_MONTH;

-- -----------------------------------------------------------------------
-- Table-count verification against expected 48,000, broken out so a
-- mismatch is easy to diagnose
-- -----------------------------------------------------------------------
SELECT customer_id, month, COUNT(*) AS categories_for_this_customer_month
FROM GOLD_CUSTOMER_CATEGORY_MONTH
GROUP BY customer_id, month
HAVING COUNT(*) <> 4
ORDER BY customer_id, month;
-- Expected: zero rows returned. Any row returned here is a customer-month
-- that does not have exactly 4 category rows and needs investigation.
