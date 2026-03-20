WITH source AS (
    SELECT * FROM {{ source('raw', 'departments') }}
),
cleaned AS (
    SELECT
        DEPARTMENT_ID,
        INITCAP(DEPARTMENT_NAME)    AS department_name,
        TRY_TO_NUMBER(FLOOR)        AS floor,
        INITCAP(BUILDING)           AS building,
        HEAD_DOCTOR_ID,
        _LOAD_TIMESTAMP             AS loaded_at
    FROM source
    WHERE DEPARTMENT_ID IS NOT NULL
)
SELECT * FROM cleaned