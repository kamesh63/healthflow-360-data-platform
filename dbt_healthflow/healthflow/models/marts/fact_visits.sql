-- CONCEPT: Fact tables join all dimensions together
-- We use ref() for staging models — dbt handles schema resolution
-- LEFT JOIN ensures visits aren't lost if a dim record is missing
-- This is called "fact table grain" — one row per visit

WITH visits AS (
    SELECT * FROM {{ ref('stg_appointments') }}
),
patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
    WHERE IS_CURRENT = TRUE
),
doctors AS (
    SELECT * FROM {{ ref('stg_doctors') }}
),
departments AS (
    SELECT * FROM {{ ref('stg_departments') }}
)

SELECT
    -- Surrogate keys
    MD5(V.VISIT_ID)                                 AS visit_key,
    MD5(V.PATIENT_ID ||
        COALESCE(P.EFFECTIVE_DATE::STRING,''))      AS patient_key,
    MD5(V.DOCTOR_ID)                                AS doctor_key,
    MD5(D.DEPARTMENT_ID)                            AS department_key,
    TO_NUMBER(TO_CHAR(V.VISIT_DATE,'YYYYMMDD'))     AS date_key,

    -- Degenerate dimensions
    V.VISIT_ID,
    V.PATIENT_ID,
    V.DOCTOR_ID,
    V.DIAGNOSIS_CODE,
    V.DIAGNOSIS_DESC,

    -- Facts
    V.VISIT_TYPE,
    V.STATUS,
    V.DURATION_MINUTES,
    V.IS_COMPLETED,
    V.FOLLOW_UP_NEEDED,
    CASE WHEN V.IS_COMPLETED THEN 1 ELSE 0 END     AS completed_flag,
    CASE WHEN V.FOLLOW_UP_NEEDED THEN 1 ELSE 0 END AS followup_flag,

    -- Conformed date attributes
    V.VISIT_DATE,
    V.VISIT_YEAR,
    V.VISIT_MONTH,
    V.VISIT_QUARTER,
    V.VISIT_DAY_NAME,

    V.LOADED_AT

FROM visits V
LEFT JOIN patients    P ON V.PATIENT_ID  = P.PATIENT_ID
LEFT JOIN doctors     DR ON V.DOCTOR_ID  = DR.DOCTOR_ID
LEFT JOIN departments D
       ON UPPER(V.DEPARTMENT) = UPPER(D.DEPARTMENT_NAME)