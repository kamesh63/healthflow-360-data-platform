-- ============================================================
-- HealthFlow 360 — Governance & Security
-- Implements HIPAA-compliant data access controls
-- ============================================================

USE WAREHOUSE HEALTHFLOW_WH;
USE DATABASE  HEALTHFLOW_DB;

-- ============================================================
-- STEP 6.1: ROW LEVEL SECURITY
-- CONCEPT: Row Access Policy — a function that returns TRUE
-- or FALSE for each row. Snowflake evaluates this function
-- invisibly before returning results. The user never knows
-- rows are being filtered — it just looks like less data.
-- ============================================================

-- First create a mapping table: which doctor sees which patients
CREATE OR REPLACE TABLE ANALYTICS.DOCTOR_PATIENT_ACCESS (
    DOCTOR_ID  VARCHAR(20),
    PATIENT_ID VARCHAR(20)
);

-- Populate with sample access rules
-- In production this would come from the EMR system
INSERT INTO ANALYTICS.DOCTOR_PATIENT_ACCESS
SELECT DISTINCT
    DOCTOR_ID,
    PATIENT_ID
FROM ANALYTICS.FACT_VISITS
LIMIT 10000;  -- Sample for demo

-- Create the Row Access Policy
-- CONCEPT: CURRENT_ROLE() and CURRENT_USER() are Snowflake
-- session functions — they return who is running the query.
-- The policy uses these to decide what rows to show.
CREATE OR REPLACE ROW ACCESS POLICY ANALYTICS.HEALTHFLOW_VISIT_POLICY
AS (PATIENT_ID VARCHAR) RETURNS BOOLEAN ->
    -- ACCOUNTADMIN and HEALTHFLOW_ADMIN see everything
    CURRENT_ROLE() IN ('ACCOUNTADMIN','HEALTHFLOW_ADMIN')

    OR

    -- HEALTHFLOW_ANALYST sees all rows
    CURRENT_ROLE() = 'HEALTHFLOW_ANALYST'

    OR

    -- Doctors see only their assigned patients
    (
        CURRENT_ROLE() = 'HEALTHFLOW_DEVELOPER'
        AND EXISTS (
            SELECT 1
            FROM ANALYTICS.DOCTOR_PATIENT_ACCESS DAP
            WHERE DAP.PATIENT_ID = PATIENT_ID
            AND   DAP.DOCTOR_ID  = CURRENT_USER()
        )
    );

-- Apply the policy to FACT_VISITS
ALTER TABLE ANALYTICS.FACT_VISITS
    ADD ROW ACCESS POLICY ANALYTICS.HEALTHFLOW_VISIT_POLICY
    ON (PATIENT_ID);

-- Verify policy is applied
SHOW ROW ACCESS POLICIES;
SELECT * FROM INFORMATION_SCHEMA.POLICY_REFERENCES
WHERE POLICY_NAME = 'HEALTHFLOW_VISIT_POLICY';

-- ============================================================
-- STEP 6.2: COLUMN MASKING (Dynamic Data Masking)
-- CONCEPT: Sensitive columns show real values to admins
-- but masked values to everyone else.
-- The masking happens at query time — data is stored as-is.
-- No performance impact, no data duplication.
-- ============================================================

-- Masking Policy for EMAIL
-- Analysts see: j***@***.com instead of john@gmail.com
CREATE OR REPLACE MASKING POLICY ANALYTICS.MASK_EMAIL
AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','HEALTHFLOW_ADMIN')
        THEN VAL  -- Show real email
        ELSE
            -- Show first char + *** + @ + *** + .com
            LEFT(VAL,1) || '***@***.***'
    END;

-- Masking Policy for PHONE
CREATE OR REPLACE MASKING POLICY ANALYTICS.MASK_PHONE
AS (VAL STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','HEALTHFLOW_ADMIN')
        THEN VAL
        ELSE '***-***-' || RIGHT(VAL,4)  -- Show last 4 digits only
    END;

-- Masking Policy for DATE OF BIRTH
-- Analysts see birth YEAR only, not full date
CREATE OR REPLACE MASKING POLICY ANALYTICS.MASK_DOB
AS (VAL DATE) RETURNS DATE ->
    CASE
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','HEALTHFLOW_ADMIN')
        THEN VAL
        ELSE DATE_FROM_PARTS(YEAR(VAL),1,1)  -- Return Jan 1 of birth year
    END;

-- Apply masking policies to DIM_PATIENT
ALTER TABLE ANALYTICS.DIM_PATIENT
    MODIFY COLUMN EMAIL
    SET MASKING POLICY ANALYTICS.MASK_EMAIL;

ALTER TABLE ANALYTICS.DIM_PATIENT
    MODIFY COLUMN PHONE
    SET MASKING POLICY ANALYTICS.MASK_PHONE;

ALTER TABLE ANALYTICS.DIM_PATIENT
    MODIFY COLUMN DOB
    SET MASKING POLICY ANALYTICS.MASK_DOB;

-- Verify masking policies
SHOW MASKING POLICIES;

-- Test masking as ACCOUNTADMIN (should see real values)
SELECT
    PATIENT_ID,
    FULL_NAME,
    EMAIL,
    PHONE,
    DOB
FROM ANALYTICS.DIM_PATIENT
LIMIT 5;

-- ============================================================
-- STEP 6.3: TIME TRAVEL
-- CONCEPT: Snowflake keeps historical versions of your data
-- for up to 90 days (Enterprise) or 1 day (Standard).
-- You can query data AS OF a past timestamp or before
-- a specific statement ran. Critical for:
--   - Recovering accidentally deleted/updated data
--   - Auditing what data looked like at a point in time
--   - Debugging pipeline issues
-- ============================================================

-- Set retention period (days)
ALTER TABLE ANALYTICS.FACT_VISITS
    SET DATA_RETENTION_TIME_IN_DAYS = 7;

ALTER TABLE ANALYTICS.DIM_PATIENT
    SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Simulate an accidental delete
-- STEP 1: Note current row count
SELECT COUNT(*) AS before_delete FROM ANALYTICS.FACT_VISITS;

-- STEP 2: Save current timestamp
-- (copy this value — you'll need it to recover)
SELECT CURRENT_TIMESTAMP() AS saved_timestamp;

-- STEP 3: Accidentally delete some rows
DELETE FROM ANALYTICS.FACT_VISITS
WHERE VISIT_YEAR = 2021
AND   VISIT_MONTH = 1;

-- STEP 4: Panic — check row count
SELECT COUNT(*) AS after_delete FROM ANALYTICS.FACT_VISITS;

-- STEP 5: Recover using Time Travel
-- Query data before the delete happened
-- Replace the timestamp below with your saved_timestamp
CREATE OR REPLACE TABLE ANALYTICS.FACT_VISITS AS
SELECT * FROM ANALYTICS.FACT_VISITS
BEFORE (STATEMENT => LAST_QUERY_ID(-2));

-- STEP 6: Verify recovery
SELECT COUNT(*) AS after_recovery FROM ANALYTICS.FACT_VISITS;

-- CONCEPT: UNDROP — recover entire dropped tables
-- If someone runs DROP TABLE accidentally:
-- DROP TABLE ANALYTICS.DIM_PATIENT;  -- oops!
-- UNDROP TABLE ANALYTICS.DIM_PATIENT; -- recovered!

-- ============================================================
-- STEP 6.4: STREAMS & TASKS (Change Data Capture)
-- CONCEPT: A Stream tracks changes (INSERT/UPDATE/DELETE)
-- to a table since the last time it was consumed.
-- A Task runs SQL on a schedule automatically.
-- Together = automated incremental processing pipeline.
-- ============================================================

-- Create a stream on RAW.APPOINTMENTS
-- This tracks every new row added to RAW.APPOINTMENTS
CREATE OR REPLACE STREAM RAW.APPOINTMENTS_STREAM
    ON TABLE RAW.APPOINTMENTS
    APPEND_ONLY = TRUE  -- Only track INSERTs (not updates/deletes)
    COMMENT = 'Tracks new appointments loaded into RAW layer';

-- Check stream (empty until new data arrives)
SELECT SYSTEM$STREAM_HAS_DATA('RAW.APPOINTMENTS_STREAM') AS has_new_data;

-- Create a Task that processes the stream every hour
-- CONCEPT: Tasks are Snowflake's built-in scheduler
-- No external scheduler needed (no cron, no Airflow)
CREATE OR REPLACE TASK RAW.PROCESS_NEW_APPOINTMENTS
    WAREHOUSE = HEALTHFLOW_WH
    SCHEDULE  = '60 MINUTE'  -- Run every 60 minutes

    -- Only run if stream has new data
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.APPOINTMENTS_STREAM')

AS
    -- Merge new appointments into STAGING layer
    MERGE INTO STAGING.APPOINTMENTS TGT
    USING (
        SELECT
            VISIT_ID,
            PATIENT_ID,
            DOCTOR_ID,
            INITCAP(DEPARTMENT)                          AS DEPARTMENT,
            TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD')        AS VISIT_DATE,
            YEAR(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD'))  AS VISIT_YEAR,
            MONTH(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD')) AS VISIT_MONTH,
            'Q'||CEIL(MONTH(TRY_TO_DATE(
                VISIT_DATE,'YYYY-MM-DD'))/3)             AS VISIT_QUARTER,
            INITCAP(VISIT_TYPE)                          AS VISIT_TYPE,
            UPPER(DIAGNOSIS_CODE)                        AS DIAGNOSIS_CODE,
            INITCAP(STATUS)                              AS STATUS,
            CASE WHEN UPPER(STATUS) = 'COMPLETED'
                 THEN TRUE ELSE FALSE END                AS IS_COMPLETED,
            TRY_TO_NUMBER(DURATION_MINUTES)             AS DURATION_MINUTES,
            CASE WHEN UPPER(FOLLOW_UP_NEEDED) = 'Y'
                 THEN TRUE ELSE FALSE END                AS FOLLOW_UP_NEEDED,
            _LOAD_TIMESTAMP
        FROM RAW.APPOINTMENTS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ) SRC ON TGT.VISIT_ID = SRC.VISIT_ID

    WHEN MATCHED THEN UPDATE SET
        TGT.STATUS        = SRC.STATUS,
        TGT.IS_COMPLETED  = SRC.IS_COMPLETED,
        TGT.LOADED_AT     = SRC._LOAD_TIMESTAMP

    WHEN NOT MATCHED THEN INSERT (
        VISIT_ID, PATIENT_ID, DOCTOR_ID, DEPARTMENT,
        VISIT_DATE, VISIT_YEAR, VISIT_MONTH, VISIT_QUARTER,
        VISIT_TYPE, DIAGNOSIS_CODE, STATUS, IS_COMPLETED,
        DURATION_MINUTES, FOLLOW_UP_NEEDED, LOADED_AT
    ) VALUES (
        SRC.VISIT_ID, SRC.PATIENT_ID, SRC.DOCTOR_ID, SRC.DEPARTMENT,
        SRC.VISIT_DATE, SRC.VISIT_YEAR, SRC.VISIT_MONTH, SRC.VISIT_QUARTER,
        SRC.VISIT_TYPE, SRC.DIAGNOSIS_CODE, SRC.STATUS, SRC.IS_COMPLETED,
        SRC.DURATION_MINUTES, SRC.FOLLOW_UP_NEEDED, SRC._LOAD_TIMESTAMP
    );

-- Resume the task (tasks start suspended by default)
ALTER TASK RAW.PROCESS_NEW_APPOINTMENTS RESUME;

-- Verify task is running
SHOW TASKS;

-- ============================================================
-- STEP 6.5: RESOURCE MONITORS
-- CONCEPT: Resource Monitors set credit usage limits.
-- If your warehouse burns through credits too fast
-- (runaway query, accidental full table scan on 5M rows),
-- the monitor automatically suspends the warehouse.
-- Critical for cost control in production.
-- ============================================================

-- Create a Resource Monitor for our warehouse
CREATE OR REPLACE RESOURCE MONITOR HEALTHFLOW_MONITOR
    WITH
        CREDIT_QUOTA    = 20        -- Max 20 credits per month
        FREQUENCY       = MONTHLY   -- Reset counter monthly
        START_TIMESTAMP = IMMEDIATELY

    -- Triggers at different usage thresholds
    TRIGGERS
        ON 50  PERCENT DO NOTIFY          -- Email at 50% usage
        ON 75  PERCENT DO NOTIFY          -- Email at 75% usage
        ON 90  PERCENT DO SUSPEND         -- Suspend at 90%
        ON 100 PERCENT DO SUSPEND_IMMEDIATE; -- Force suspend at 100%

-- Apply monitor to our warehouse
ALTER WAREHOUSE HEALTHFLOW_WH
    SET RESOURCE_MONITOR = HEALTHFLOW_MONITOR;

-- ============================================================
-- STEP 6.6: QUERY PERFORMANCE & CLUSTERING
-- CONCEPT: Snowflake has 3 caching layers:
--   1. Metadata cache   — table stats, min/max values
--   2. Result cache     — exact same query = instant result
--   3. Virtual warehouse cache — data pages in SSD memory
--
-- Clustering keys tell Snowflake how to organize micro-partitions
-- so queries with WHERE on those columns skip irrelevant data.
-- ============================================================

-- Add clustering key to FACT_VISITS
-- Queries filtering by VISIT_YEAR or DEPARTMENT will be faster
ALTER TABLE ANALYTICS.FACT_VISITS
    CLUSTER BY (VISIT_YEAR, VISIT_MONTH);

ALTER TABLE ANALYTICS.FACT_BILLING
    CLUSTER BY (BILLING_DATE);

ALTER TABLE ANALYTICS.FACT_LAB_RESULTS
    CLUSTER BY (DEPARTMENT, TEST_DATE);

-- Check clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION(
    'ANALYTICS.FACT_VISITS',
    '(VISIT_YEAR, VISIT_MONTH)'
);

-- Test result cache
-- Run same query twice — second run should be instant
SELECT
    VISIT_YEAR,
    VISIT_QUARTER,
    COUNT(*)          AS total_visits,
    AVG(DURATION_MINUTES) AS avg_duration
FROM ANALYTICS.FACT_VISITS
GROUP BY 1,2
ORDER BY 1,2;

-- Run it again immediately — check query profile
-- It should say "Query result reused"
SELECT
    VISIT_YEAR,
    VISIT_QUARTER,
    COUNT(*)          AS total_visits,
    AVG(DURATION_MINUTES) AS avg_duration
FROM ANALYTICS.FACT_VISITS
GROUP BY 1,2
ORDER BY 1,2;

-- Final verification of all governance objects
SHOW ROW ACCESS POLICIES   IN DATABASE HEALTHFLOW_DB;
SHOW MASKING POLICIES      IN DATABASE HEALTHFLOW_DB;
SHOW STREAMS               IN DATABASE HEALTHFLOW_DB;
SHOW TASKS                 IN DATABASE HEALTHFLOW_DB;
SHOW RESOURCE MONITORS;