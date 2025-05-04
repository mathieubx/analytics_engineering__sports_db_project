WITH 

days AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2000-01-01' AS DATE)",
        end_date="CAST('2050-01-01' AS DATE)"
        )
    }}
),

months AS (
    SELECT 1 AS month_int, 'January' AS month_name, 'Jan' AS month_short_name
    UNION ALL
    SELECT 2, 'February', 'Feb'
    UNION ALL
    SELECT 3, 'March', 'Mar'
    UNION ALL
    SELECT 4, 'April', 'Apr'
    UNION ALL
    SELECT 5, 'May', 'May'
    UNION ALL
    SELECT 6, 'June', 'Jun'
    UNION ALL
    SELECT 7, 'July', 'Jul'
    UNION ALL
    SELECT 8, 'August', 'Aug'
    UNION ALL
    SELECT 9, 'September', 'Sep'
    UNION ALL
    SELECT 10, 'October', 'Oct'
    UNION ALL
    SELECT 11, 'November', 'Nov'
    UNION ALL
    SELECT 12, 'December', 'Dec'
),

days_weeks_months_years AS (
    SELECT
        date_day,
        WEEK(date_day) AS date_week,
        EXTRACT(MONTH FROM date_day) AS date_month,
        EXTRACT(YEAR FROM date_day) AS date_year,
        -- Computing start of the week and end of the week dates
        CASE 
            -- A conditional statement is needed because Sunday's dayofweek in DuckDB is 0, which generates errors in the formula I created.
            -- The error is the following: on sundays, [date_day - INTERVAL (DATE_PART('dayofweek', date_day) - 1) DAY] = [date_day - INTERVAL (0 - 1) DAY] = [date_day + INTERVAL 1 DAY] while it should be [date_day - INTERVAL 6 DAYS]
            -- To prevent the formula to generate start of the week to be on next week, we use this condition
            WHEN DATE_PART('dayofweek', date_day) = 0 THEN date_day - INTERVAL 6 DAY 
            WHEN DATE_PART('dayofweek', date_day) != 0 THEN date_day - INTERVAL (DATE_PART('dayofweek', date_day) - 1) DAY
        END	AS week_start_date,
        CASE 
            WHEN DATE_PART('dayofweek', date_day) = 0 THEN date_day
            WHEN DATE_PART('dayofweek', date_day) != 0 THEN date_day + INTERVAL (7 - DATE_PART('dayofweek', date_day)) DAY 
        END	AS week_end_date,            
    FROM days
)

SELECT
    dwmy.date_day,
    dwmy.date_week,
    dwmy.week_start_date,
    dwmy.week_end_date,
    dwmy.date_month,
    m.month_name,
    m.month_short_name,
    dwmy.date_year
FROM days_weeks_months_years AS dwmy
LEFT JOIN months AS m ON dwmy.date_month = m.month_int
