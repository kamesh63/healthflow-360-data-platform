-- ============================================================
-- HealthFlow 360 — Stage Creation & Data Loading
-- CONCEPT: Stage → COPY INTO is Snowflake's fastest load method
-- Handles compression, parallelism, and error recovery
-- ============================================================

USE WAREHOUSE HEALTHFLOW_WH;
USE DATABASE  HEALTHFLOW_DB;
USE SCHEMA    RAW;

-- ── STEP 1: Create a Named Internal Stage ───────────────────
-- CONCEPT: A stage is a pointer to a file location.
-- Internal stage = Snowflake manages the storage for you.
-- Files uploaded here are compressed and encrypted automatically.
CREATE OR REPLACE STAGE HEALTHFLOW_RAW_STAGE
    COMMENT = 'Internal stage for HealthFlow raw CSV files';

-- ── STEP 2: Create File Format ───────────────────────────────
-- CONCEPT: File format tells Snowflake exactly how to parse
-- your files — delimiter, header row, how to handle nulls,
-- what to do with empty strings, date formats etc.
-- One format object reused across all 6 tables = DRY principle.
CREATE OR REPLACE FILE FORMAT HEALTHFLOW_CSV_FORMAT
    TYPE                = 'CSV'
    FIELD_DELIMITER     = ','
    RECORD_DELIMITER    = '\n'
    SKIP_HEADER         = 1           -- Skip the header row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF             = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE          = TRUE
    DATE_FORMAT         = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT    = 'YYYY-MM-DD HH24:MI:SS'
    COMMENT             = 'Standard CSV format for HealthFlow files';

-- Verify stage and format created
SHOW STAGES;
SHOW FILE FORMATS;

SELECT CURRENT_ACCOUNT();
SELECT CURRENT_USER();
SELECT CURRENT_REGION();