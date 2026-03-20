WITH source AS (
    SELECT * FROM {{ source('raw', 'doctors') }}
),
cleaned AS (
    SELECT
        DOCTOR_ID,
        INITCAP(FIRST_NAME)                             AS first_name,
        INITCAP(LAST_NAME)                              AS last_name,
        INITCAP(FIRST_NAME)||' '||INITCAP(LAST_NAME)   AS full_name,
        INITCAP(SPECIALIZATION)                         AS specialization,
        INITCAP(DEPARTMENT)                             AS department,
        UPPER(QUALIFICATION)                            AS qualification,
        TRY_TO_DATE(HIRE_DATE,'YYYY-MM-DD')             AS hire_date,
        TRY_CAST(EXPERIENCE_YEARS AS INTEGER)           AS experience_years,
        TRY_TO_DECIMAL(CONSULTATION_FEE,10,2)           AS consultation_fee,
        CASE WHEN UPPER(IS_ACTIVE) = 'Y'
             THEN TRUE ELSE FALSE END                   AS is_active,
        TRY_TO_TIMESTAMP(CREATED_AT,
            'YYYY-MM-DD HH24:MI:SS')                   AS created_at,
        _LOAD_TIMESTAMP                                 AS loaded_at
    FROM source
    WHERE DOCTOR_ID IS NOT NULL
)
SELECT * FROM cleaned