-- ============================================================
-- HealthFlow 360 — STAGING Layer (Silver)
-- CONCEPT: CTAS — Create Table As Select
-- One statement creates AND populates the table simultaneously
-- Much faster than CREATE then INSERT separately
-- ============================================================

USE WAREHOUSE HEALTHFLOW_WH;
USE DATABASE  HEALTHFLOW_DB;

-- ── STAGING.PATIENTS ─────────────────────────────────────────
-- CONCEPT: SCD Type 2 tracking columns added here
-- We keep effective_date and expiry_date to track
-- when a patient's insurance or address changed over time
CREATE OR REPLACE TABLE STAGING.PATIENTS AS
SELECT
    -- Natural key
    PATIENT_ID,

    -- Personal info — cleaned and standardized
    INITCAP(FIRST_NAME)                         AS FIRST_NAME,
    INITCAP(LAST_NAME)                          AS LAST_NAME,
    INITCAP(FIRST_NAME)||' '||INITCAP(LAST_NAME) AS FULL_NAME,

    -- CONCEPT: TRY_TO_DATE — safe casting
    -- Unlike TO_DATE, it returns NULL instead of erroring
    -- on bad values. Always use TRY_ variants in production.
    TRY_TO_DATE(DOB, 'YYYY-MM-DD')             AS DOB,

    -- Derived: Age calculated from DOB
    DATEDIFF('year',
        TRY_TO_DATE(DOB,'YYYY-MM-DD'),
        CURRENT_DATE())                          AS AGE,

    -- Derived: Age group for analytics segmentation
    CASE
        WHEN DATEDIFF('year',
             TRY_TO_DATE(DOB,'YYYY-MM-DD'),
             CURRENT_DATE()) < 18  THEN 'Pediatric'
        WHEN DATEDIFF('year',
             TRY_TO_DATE(DOB,'YYYY-MM-DD'),
             CURRENT_DATE()) < 40  THEN 'Young Adult'
        WHEN DATEDIFF('year',
             TRY_TO_DATE(DOB,'YYYY-MM-DD'),
             CURRENT_DATE()) < 60  THEN 'Middle Aged'
        WHEN DATEDIFF('year',
             TRY_TO_DATE(DOB,'YYYY-MM-DD'),
             CURRENT_DATE()) < 80  THEN 'Senior'
        ELSE 'Elderly'
    END                                          AS AGE_GROUP,

    -- Validated gender
    CASE
        WHEN UPPER(GENDER) IN ('M','F','O') THEN UPPER(GENDER)
        ELSE 'U'
    END                                          AS GENDER,

    -- Location
    INITCAP(CITY)                               AS CITY,
    UPPER(STATE)                                AS STATE,
    ZIP_CODE,

    -- Insurance
    INITCAP(INSURANCE_TYPE)                     AS INSURANCE_TYPE,
    UPPER(BLOOD_GROUP)                          AS BLOOD_GROUP,

    -- Contact
    PHONE,
    LOWER(EMAIL)                                AS EMAIL,

    -- SCD Type 2 columns
    TRY_TO_DATE(EFFECTIVE_DATE,'YYYY-MM-DD')   AS EFFECTIVE_DATE,
    TRY_TO_DATE(EXPIRY_DATE,  'YYYY-MM-DD')   AS EXPIRY_DATE,
    CASE WHEN UPPER(IS_CURRENT) = 'Y'
         THEN TRUE ELSE FALSE END               AS IS_CURRENT,

    -- Audit
    TRY_TO_TIMESTAMP(CREATED_AT,
        'YYYY-MM-DD HH24:MI:SS')               AS CREATED_AT,
    _SOURCE_FILE,
    TRY_TO_DATE(_INGESTION_DATE,'YYYY-MM-DD') AS INGESTION_DATE,
    _LOAD_TIMESTAMP

FROM RAW.PATIENTS
WHERE PATIENT_ID IS NOT NULL;

-- ── STAGING.DOCTORS ──────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.DOCTORS AS
SELECT
    DOCTOR_ID,
    INITCAP(FIRST_NAME)                          AS FIRST_NAME,
    INITCAP(LAST_NAME)                           AS LAST_NAME,
    INITCAP(FIRST_NAME)||' '||INITCAP(LAST_NAME) AS FULL_NAME,
    INITCAP(SPECIALIZATION)                      AS SPECIALIZATION,
    INITCAP(DEPARTMENT)                          AS DEPARTMENT,
    UPPER(QUALIFICATION)                         AS QUALIFICATION,
    TRY_TO_DATE(HIRE_DATE,'YYYY-MM-DD')         AS HIRE_DATE,

    -- Years of experience validated (must be positive)
    CASE
        WHEN TRY_TO_NUMBER(EXPERIENCE_YEARS) >= 0
        THEN TRY_TO_NUMBER(EXPERIENCE_YEARS)
        ELSE NULL
    END                                          AS EXPERIENCE_YEARS,

    -- Fee validated (must be positive)
    CASE
        WHEN TRY_TO_DECIMAL(CONSULTATION_FEE,10,2) > 0
        THEN TRY_TO_DECIMAL(CONSULTATION_FEE,10,2)
        ELSE NULL
    END                                          AS CONSULTATION_FEE,

    CASE WHEN UPPER(IS_ACTIVE) = 'Y'
         THEN TRUE ELSE FALSE END                AS IS_ACTIVE,

    TRY_TO_TIMESTAMP(CREATED_AT,
        'YYYY-MM-DD HH24:MI:SS')                AS CREATED_AT,
    _SOURCE_FILE,
    TRY_TO_DATE(_INGESTION_DATE,'YYYY-MM-DD')  AS INGESTION_DATE,
    _LOAD_TIMESTAMP

FROM RAW.DOCTORS
WHERE DOCTOR_ID IS NOT NULL;

-- ── STAGING.DEPARTMENTS ──────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.DEPARTMENTS AS
SELECT
    DEPARTMENT_ID,
    INITCAP(DEPARTMENT_NAME)   AS DEPARTMENT_NAME,
    TRY_TO_NUMBER(FLOOR)       AS FLOOR,
    INITCAP(BUILDING)          AS BUILDING,
    HEAD_DOCTOR_ID,
    TRY_TO_DATE(CREATED_AT,
        'YYYY-MM-DD')          AS CREATED_AT,
    _LOAD_TIMESTAMP

FROM RAW.DEPARTMENTS
WHERE DEPARTMENT_ID IS NOT NULL;

-- ── STAGING.APPOINTMENTS ─────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.APPOINTMENTS AS
SELECT
    VISIT_ID,
    PATIENT_ID,
    DOCTOR_ID,
    INITCAP(DEPARTMENT)                        AS DEPARTMENT,
    TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD')      AS VISIT_DATE,

    -- Derived date parts — used heavily in analytics
    YEAR(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD')) AS VISIT_YEAR,
    MONTH(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD'))AS VISIT_MONTH,
    DAY(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD'))  AS VISIT_DAY,
    DAYNAME(TRY_TO_DATE(VISIT_DATE,
        'YYYY-MM-DD'))                          AS VISIT_DAY_NAME,
    'Q'||CEIL(MONTH(TRY_TO_DATE(VISIT_DATE,
        'YYYY-MM-DD'))/3)                       AS VISIT_QUARTER,

    INITCAP(VISIT_TYPE)                        AS VISIT_TYPE,
    UPPER(DIAGNOSIS_CODE)                      AS DIAGNOSIS_CODE,
    INITCAP(DIAGNOSIS_DESC)                    AS DIAGNOSIS_DESC,
    INITCAP(STATUS)                            AS STATUS,

    -- Validated duration
    CASE
        WHEN TRY_TO_NUMBER(DURATION_MINUTES)
             BETWEEN 1 AND 480
        THEN TRY_TO_NUMBER(DURATION_MINUTES)
        ELSE NULL
    END                                        AS DURATION_MINUTES,

    CASE WHEN UPPER(FOLLOW_UP_NEEDED) = 'Y'
         THEN TRUE ELSE FALSE END              AS FOLLOW_UP_NEEDED,
    CASE WHEN UPPER(STATUS) = 'COMPLETED'
         THEN TRUE ELSE FALSE END              AS IS_COMPLETED,

    TRY_TO_TIMESTAMP(CREATED_AT,
        'YYYY-MM-DD HH24:MI:SS')              AS CREATED_AT,
    _SOURCE_FILE,
    TRY_TO_DATE(_INGESTION_DATE,'YYYY-MM-DD') AS INGESTION_DATE,
    _LOAD_TIMESTAMP

FROM RAW.APPOINTMENTS
WHERE VISIT_ID IS NOT NULL;

-- ── STAGING.LAB_RESULTS ──────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.LAB_RESULTS AS
SELECT
    LAB_ID,
    VISIT_ID,
    PATIENT_ID,
    INITCAP(DEPARTMENT)                        AS DEPARTMENT,
    INITCAP(TEST_NAME)                         AS TEST_NAME,
    TRY_TO_DATE(TEST_DATE,'YYYY-MM-DD')       AS TEST_DATE,
    RESULT_VALUE,

    -- Try to cast result to number where possible
    TRY_TO_DECIMAL(RESULT_VALUE, 10, 2)       AS RESULT_VALUE_NUM,

    -- Flag text vs numeric results
    CASE
        WHEN TRY_TO_DECIMAL(RESULT_VALUE,10,2)
             IS NOT NULL THEN 'NUMERIC'
        ELSE 'TEXT'
    END                                        AS RESULT_TYPE,

    UNIT,
    NORMAL_RANGE,
    CASE WHEN UPPER(IS_ABNORMAL) = 'Y'
         THEN TRUE ELSE FALSE END              AS IS_ABNORMAL,
    CASE WHEN UPPER(IS_ABNORMAL) = 'Y'
         THEN 1 ELSE 0 END                    AS IS_ABNORMAL_FLAG,
    REVIEWED_BY,
    TRY_TO_TIMESTAMP(CREATED_AT,
        'YYYY-MM-DD HH24:MI:SS')              AS CREATED_AT,
    _SOURCE_FILE,
    TRY_TO_DATE(_INGESTION_DATE,'YYYY-MM-DD') AS INGESTION_DATE,
    _LOAD_TIMESTAMP

FROM RAW.LAB_RESULTS
WHERE LAB_ID IS NOT NULL;

-- ── STAGING.BILLING ──────────────────────────────────────────
CREATE OR REPLACE TABLE STAGING.BILLING AS
SELECT
    BILLING_ID,
    VISIT_ID,
    PATIENT_ID,
    TRY_TO_DATE(BILLING_DATE,'YYYY-MM-DD')    AS BILLING_DATE,
    INITCAP(VISIT_TYPE)                        AS VISIT_TYPE,
    INITCAP(INSURANCE_TYPE)                    AS INSURANCE_TYPE,

    -- Financial amounts validated
    CASE WHEN TRY_TO_DECIMAL(TOTAL_AMOUNT,12,2) > 0
         THEN TRY_TO_DECIMAL(TOTAL_AMOUNT,12,2)
         ELSE NULL END                         AS TOTAL_AMOUNT,

    CASE WHEN TRY_TO_DECIMAL(INSURANCE_COVERED,12,2) >= 0
         THEN TRY_TO_DECIMAL(INSURANCE_COVERED,12,2)
         ELSE NULL END                         AS INSURANCE_COVERED,

    CASE WHEN TRY_TO_DECIMAL(PATIENT_DUE,12,2) >= 0
         THEN TRY_TO_DECIMAL(PATIENT_DUE,12,2)
         ELSE NULL END                         AS PATIENT_DUE,

    -- Coverage percentage derived
    ROUND(
        TRY_TO_DECIMAL(INSURANCE_COVERED,12,2) /
        NULLIF(TRY_TO_DECIMAL(TOTAL_AMOUNT,12,2), 0) * 100
    , 2)                                       AS COVERAGE_PCT,

    INITCAP(PAYMENT_STATUS)                    AS PAYMENT_STATUS,
    INITCAP(PAYMENT_METHOD)                    AS PAYMENT_METHOD,

    -- Risk flags
    CASE WHEN UPPER(PAYMENT_STATUS) = 'PAID'
         THEN TRUE ELSE FALSE END              AS IS_PAID,
    CASE
        WHEN UPPER(PAYMENT_STATUS) = 'OVERDUE' THEN 'HIGH'
        WHEN UPPER(PAYMENT_STATUS) = 'PENDING' THEN 'MEDIUM'
        ELSE 'LOW'
    END                                        AS COLLECTION_RISK,

    TRY_TO_TIMESTAMP(CREATED_AT,
        'YYYY-MM-DD HH24:MI:SS')              AS CREATED_AT,
    _SOURCE_FILE,
    TRY_TO_DATE(_INGESTION_DATE,'YYYY-MM-DD') AS INGESTION_DATE,
    _LOAD_TIMESTAMP

FROM RAW.BILLING
WHERE BILLING_ID IS NOT NULL;

-- ── VERIFY ALL STAGING TABLES ────────────────────────────────
SELECT 'PATIENTS'     AS table_name, COUNT(*) AS rows,
       COUNT(AGE)     AS age_populated,
       COUNT(DOB)     AS dob_cast_ok
FROM STAGING.PATIENTS  UNION ALL

SELECT 'DOCTORS',      COUNT(*), COUNT(HIRE_DATE),
       COUNT(CONSULTATION_FEE)
FROM STAGING.DOCTORS   UNION ALL

SELECT 'DEPARTMENTS',  COUNT(*), COUNT(FLOOR), COUNT(FLOOR)
FROM STAGING.DEPARTMENTS UNION ALL

SELECT 'APPOINTMENTS', COUNT(*), COUNT(VISIT_DATE),
       COUNT(VISIT_QUARTER)
FROM STAGING.APPOINTMENTS UNION ALL

SELECT 'LAB_RESULTS',  COUNT(*), COUNT(TEST_DATE),
       COUNT(IS_ABNORMAL_FLAG)
FROM STAGING.LAB_RESULTS UNION ALL

SELECT 'BILLING',      COUNT(*), COUNT(TOTAL_AMOUNT),
       COUNT(COVERAGE_PCT)
FROM STAGING.BILLING;