-- CONCEPT: ref() function — dbt's dependency manager
-- ref('stg_patients') tells dbt:
--   1. This model DEPENDS on stg_patients
--   2. Build stg_patients FIRST, then this
--   3. Resolve the correct schema automatically
-- dbt builds a DAG from all ref() calls and executes
-- models in the correct order automatically

WITH stg AS (
    SELECT * FROM {{ ref('stg_patients') }}
)
SELECT
    MD5(PATIENT_ID || EFFECTIVE_DATE::STRING)   AS patient_key,
    PATIENT_ID,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    DOB,
    AGE,
    AGE_GROUP,
    GENDER,
    CITY,
    STATE,
    ZIP_CODE,
    INSURANCE_TYPE,
    BLOOD_GROUP,
    EMAIL,
    PHONE,
    EFFECTIVE_DATE,
    EXPIRY_DATE,
    IS_CURRENT,
    CREATED_AT,
    LOADED_AT
FROM stg