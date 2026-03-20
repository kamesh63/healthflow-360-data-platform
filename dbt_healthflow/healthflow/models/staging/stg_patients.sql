-- ============================================================
-- CONCEPT: Staging models are 1-to-1 with source tables.
-- One staging model per source table. No joins here.
-- Just renaming, casting, and light cleaning.
-- The ref() and source() functions are dbt's superpowers:
--   source('raw','patients') → RAW.PATIENTS
--   ref('stg_patients')      → whatever schema stg_patients is in
-- dbt resolves these at runtime — no hardcoded paths.
-- ============================================================

WITH source AS (
    -- CONCEPT: source() function registers this as a
    -- dependency in the DAG and enables freshness testing
    SELECT * FROM {{ source('raw', 'patients') }}
),

cleaned AS (
    SELECT
        PATIENT_ID,
        INITCAP(FIRST_NAME)                             AS first_name,
        INITCAP(LAST_NAME)                              AS last_name,
        INITCAP(FIRST_NAME)||' '||INITCAP(LAST_NAME)   AS full_name,
        TRY_TO_DATE(DOB, 'YYYY-MM-DD')                 AS dob,
        DATEDIFF('year',
            TRY_TO_DATE(DOB,'YYYY-MM-DD'),
            CURRENT_DATE())                             AS age,
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
        END                                             AS age_group,
        CASE WHEN UPPER(GENDER) IN ('M','F','O')
             THEN UPPER(GENDER) ELSE 'U'
        END                                             AS gender,
        INITCAP(CITY)                                   AS city,
        UPPER(STATE)                                    AS state,
        ZIP_CODE                                        AS zip_code,
        INITCAP(INSURANCE_TYPE)                         AS insurance_type,
        UPPER(BLOOD_GROUP)                              AS blood_group,
        LOWER(EMAIL)                                    AS email,
        PHONE,
        TRY_TO_DATE(EFFECTIVE_DATE,'YYYY-MM-DD')       AS effective_date,
        TRY_TO_DATE(EXPIRY_DATE,'YYYY-MM-DD')          AS expiry_date,
        CASE WHEN UPPER(IS_CURRENT) = 'Y'
             THEN TRUE ELSE FALSE END                   AS is_current,
        TRY_TO_TIMESTAMP(CREATED_AT,
            'YYYY-MM-DD HH24:MI:SS')                   AS created_at,
        _LOAD_TIMESTAMP                                 AS loaded_at

    FROM source
    WHERE PATIENT_ID IS NOT NULL
)

SELECT * FROM cleaned