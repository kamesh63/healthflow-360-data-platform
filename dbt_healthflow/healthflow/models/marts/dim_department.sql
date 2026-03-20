WITH stg AS (
    SELECT * FROM {{ ref('stg_departments') }}
)
SELECT
    MD5(DEPARTMENT_ID)      AS department_key,
    DEPARTMENT_ID,
    DEPARTMENT_NAME,
    FLOOR,
    BUILDING,
    LOADED_AT
FROM stg