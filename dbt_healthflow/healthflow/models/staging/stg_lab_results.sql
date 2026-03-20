WITH source AS (
    SELECT * FROM {{ source('raw', 'lab_results') }}
),
cleaned AS (
    SELECT
        LAB_ID,
        VISIT_ID,
        PATIENT_ID,
        INITCAP(DEPARTMENT)                     AS department,
        INITCAP(TEST_NAME)                      AS test_name,
        TRY_TO_DATE(TEST_DATE,'YYYY-MM-DD')    AS test_date,
        RESULT_VALUE,
        TRY_TO_DECIMAL(RESULT_VALUE,10,2)      AS result_value_num,
        CASE WHEN TRY_TO_DECIMAL(
                  RESULT_VALUE,10,2) IS NOT NULL
             THEN 'NUMERIC' ELSE 'TEXT'
        END                                     AS result_type,
        UNIT,
        NORMAL_RANGE,
        CASE WHEN UPPER(IS_ABNORMAL) = 'Y'
             THEN TRUE ELSE FALSE END           AS is_abnormal,
        CASE WHEN UPPER(IS_ABNORMAL) = 'Y'
             THEN 1 ELSE 0 END                 AS is_abnormal_flag,
        REVIEWED_BY,
        _LOAD_TIMESTAMP                         AS loaded_at
    FROM source
    WHERE LAB_ID IS NOT NULL
)
SELECT * FROM cleaned