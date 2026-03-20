WITH billing AS (
    SELECT * FROM {{ ref('stg_billing') }}
),
patients AS (
    SELECT * FROM {{ ref('stg_patients') }}
    WHERE IS_CURRENT = TRUE
)

SELECT
    MD5(B.BILLING_ID)                               AS billing_key,
    MD5(B.PATIENT_ID ||
        COALESCE(P.EFFECTIVE_DATE::STRING,''))      AS patient_key,
    MD5(B.VISIT_ID)                                 AS visit_key,
    TO_NUMBER(TO_CHAR(B.BILLING_DATE,'YYYYMMDD'))   AS date_key,

    B.BILLING_ID,
    B.VISIT_ID,
    B.PATIENT_ID,
    B.VISIT_TYPE,
    B.INSURANCE_TYPE,

    -- Financial measures
    B.TOTAL_AMOUNT,
    B.INSURANCE_COVERED,
    B.PATIENT_DUE,
    B.COVERAGE_PCT,
    B.TOTAL_AMOUNT - B.INSURANCE_COVERED            AS uncovered_amount,

    -- Status
    B.PAYMENT_STATUS,
    B.PAYMENT_METHOD,
    B.IS_PAID,
    B.COLLECTION_RISK,
    CASE WHEN B.IS_PAID THEN 1 ELSE 0 END          AS paid_flag,

    B.BILLING_DATE,
    B.LOADED_AT

FROM billing B
LEFT JOIN patients P ON B.PATIENT_ID = P.PATIENT_ID