WITH source AS (
    SELECT * FROM {{ source('raw', 'billing') }}
),
cleaned AS (
    SELECT
        BILLING_ID,
        VISIT_ID,
        PATIENT_ID,
        TRY_TO_DATE(BILLING_DATE,'YYYY-MM-DD')         AS billing_date,
        INITCAP(VISIT_TYPE)                             AS visit_type,
        INITCAP(INSURANCE_TYPE)                         AS insurance_type,
        TRY_TO_DECIMAL(TOTAL_AMOUNT,12,2)              AS total_amount,
        TRY_TO_DECIMAL(INSURANCE_COVERED,12,2)         AS insurance_covered,
        TRY_TO_DECIMAL(PATIENT_DUE,12,2)               AS patient_due,
        ROUND(
            TRY_TO_DECIMAL(INSURANCE_COVERED,12,2) /
            NULLIF(TRY_TO_DECIMAL(
                TOTAL_AMOUNT,12,2),0) * 100
        ,2)                                             AS coverage_pct,
        INITCAP(PAYMENT_STATUS)                         AS payment_status,
        INITCAP(PAYMENT_METHOD)                         AS payment_method,
        CASE WHEN UPPER(PAYMENT_STATUS) = 'PAID'
             THEN TRUE ELSE FALSE END                   AS is_paid,
        CASE
            WHEN UPPER(PAYMENT_STATUS) = 'OVERDUE' THEN 'High'
            WHEN UPPER(PAYMENT_STATUS) = 'PENDING' THEN 'Medium'
            ELSE 'Low'
        END                                             AS collection_risk,
        _LOAD_TIMESTAMP                                 AS loaded_at
    FROM source
    WHERE BILLING_ID IS NOT NULL
)
SELECT * FROM cleaned