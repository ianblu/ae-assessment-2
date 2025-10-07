{{ config(
    materialized="table",
) }}



WITH RECURSIVE
    dates(date_actual) AS (
        -- Anchor member (Start Date)
        SELECT '2024-01-01'
        UNION ALL
        -- Recursive member (Increment date by 1 day)
        SELECT DATE(date_actual, '+1 day')
        FROM dates
        -- Termination condition (End Date)
        WHERE date_actual < '2025-12-31'
    )
, source as (
SELECT
    -- Surrogate Key: YYYYMMDD
    CAST(STRFTIME('%Y%m%d', date_actual) AS INTEGER) AS date_key,

    date_actual,
    STRFTIME('%Y', date_actual) AS year_num,
    STRFTIME('%m', date_actual) AS month_num,
    STRFTIME('%d', date_actual) AS day_num,
    
    CASE
        WHEN CAST(STRFTIME('%m', date_actual) AS INTEGER) BETWEEN 1 AND 3 THEN 1
        WHEN CAST(STRFTIME('%m', date_actual) AS INTEGER) BETWEEN 4 AND 6 THEN 2
        WHEN CAST(STRFTIME('%m', date_actual) AS INTEGER) BETWEEN 7 AND 9 THEN 3
        ELSE 4
    END AS quarter_num,

    STRFTIME('%w', date_actual) AS day_of_week_num_sqlite, -- 0=Sun, 6=Sat
    CASE STRFTIME('%w', date_actual)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END AS day_name,
    CASE STRFTIME('%m', date_actual)
        WHEN '01' THEN 'January'
        WHEN '02' THEN 'February'
        WHEN '03' THEN 'March'
        WHEN '04' THEN 'April'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'June'
        WHEN '07' THEN 'July'
        WHEN '08' THEN 'August'
        WHEN '09' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
    END AS month_name,

    STRFTIME('%w', date_actual) AS day_of_week,

    STRFTIME('%j', date_actual) AS day_of_year,

    STRFTIME('%W', date_actual) AS week_of_year,

    CASE STRFTIME('%w', date_actual)
        WHEN '0' THEN 1 -- Sunday
        WHEN '6' THEN 1 -- Saturday
        ELSE 0
    END AS is_weekend

FROM
    dates)
    
select * from source;