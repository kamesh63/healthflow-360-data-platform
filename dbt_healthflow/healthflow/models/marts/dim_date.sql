-- CONCEPT: dbt can generate data using SQL alone
-- This date spine creates one row per day using
-- Snowflake's GENERATOR function — no source table needed
-- This is called a "seed-free dimension"

WITH date_spine AS (
    SELECT
        DATEADD('day', SEQ4(), '2021-01-01'::DATE) AS calendar_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))
)
SELECT
    TO_NUMBER(TO_CHAR(calendar_date,'YYYYMMDD'))    AS date_key,
    calendar_date                                    AS full_date,
    YEAR(calendar_date)                              AS year,
    MONTH(calendar_date)                             AS month_num,
    MONTHNAME(calendar_date)                         AS month_name,
    DAY(calendar_date)                               AS day_of_month,
    DAYNAME(calendar_date)                           AS day_name,
    DAYOFWEEK(calendar_date)                         AS day_of_week,
    WEEKOFYEAR(calendar_date)                        AS week_of_year,
    QUARTER(calendar_date)                           AS quarter_num,
    'Q'||QUARTER(calendar_date)                      AS quarter_name,
    CASE WHEN DAYOFWEEK(calendar_date)
              IN (0,6) THEN TRUE
         ELSE FALSE END                              AS is_weekend,
    CASE WHEN DAY(calendar_date) = 1
         THEN TRUE ELSE FALSE END                    AS is_month_start,
    CASE WHEN calendar_date = LAST_DAY(calendar_date)
         THEN TRUE ELSE FALSE END                    AS is_month_end,
    TO_CHAR(calendar_date,'YYYY-MM')                AS year_month
FROM date_spine
ORDER BY calendar_date