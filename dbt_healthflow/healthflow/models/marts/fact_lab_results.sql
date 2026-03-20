WITH labs AS (
    SELECT * FROM {{ ref('stg_lab_results') }}
),
patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
    WHERE IS_CURRENT = TRUE
)

SELECT
    MD5(L.LAB_ID)                                   AS lab_key,
    MD5(L.PATIENT_ID ||
        COALESCE(P.EFFECTIVE_DATE::STRING,''))      AS patient_key,
    MD5(L.VISIT_ID)                                 AS visit_key,
    TO_NUMBER(TO_CHAR(L.TEST_DATE,'YYYYMMDD'))      AS date_key,

    L.LAB_ID,
    L.VISIT_ID,
    L.PATIENT_ID,
    L.DEPARTMENT,
    L.TEST_NAME,
    L.TEST_DATE,
    L.RESULT_VALUE,
    L.RESULT_VALUE_NUM,
    L.RESULT_TYPE,
    L.UNIT,
    L.NORMAL_RANGE,
    L.IS_ABNORMAL,
    L.IS_ABNORMAL_FLAG,
    L.REVIEWED_BY,
    L.LOADED_AT

FROM labs L
LEFT JOIN patients P ON L.PATIENT_ID = P.PATIENT_ID