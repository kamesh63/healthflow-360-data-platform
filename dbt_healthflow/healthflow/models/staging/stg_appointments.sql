WITH source AS (
    SELECT * FROM {{ source('raw', 'appointments') }}
),
cleaned AS (
    SELECT
        VISIT_ID,
        PATIENT_ID,
        DOCTOR_ID,
        INITCAP(DEPARTMENT)                             AS department,
        TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD')           AS visit_date,
        YEAR(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD'))     AS visit_year,
        MONTH(TRY_TO_DATE(VISIT_DATE,'YYYY-MM-DD'))    AS visit_month,
        'Q'||CEIL(MONTH(TRY_TO_DATE(
            VISIT_DATE,'YYYY-MM-DD'))/3)               AS visit_quarter,
        DAYNAME(TRY_TO_DATE(
            VISIT_DATE,'YYYY-MM-DD'))                  AS visit_day_name,
        INITCAP(VISIT_TYPE)                            AS visit_type,
        UPPER(DIAGNOSIS_CODE)                          AS diagnosis_code,
        INITCAP(DIAGNOSIS_DESC)                        AS diagnosis_desc,
        INITCAP(STATUS)                                AS status,
        CASE WHEN UPPER(STATUS) = 'COMPLETED'
             THEN TRUE ELSE FALSE END                  AS is_completed,
        TRY_TO_NUMBER(DURATION_MINUTES)               AS duration_minutes,
        CASE WHEN UPPER(FOLLOW_UP_NEEDED) = 'Y'
             THEN TRUE ELSE FALSE END                  AS follow_up_needed,
        TRY_TO_TIMESTAMP(CREATED_AT,
            'YYYY-MM-DD HH24:MI:SS')                  AS created_at,
        _LOAD_TIMESTAMP                                AS loaded_at
    FROM source
    WHERE VISIT_ID IS NOT NULL
)
SELECT * FROM cleaned