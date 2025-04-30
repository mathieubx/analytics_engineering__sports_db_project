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
    FROM days
)

SELECT
    dwmy.date_day,
    dwmy.date_week,
    dwmy.date_month,
    m.month_name,
    m.month_short_name,
    dwmy.date_year
FROM days_weeks_months_years AS dwmy
LEFT JOIN months AS m ON dwmy.date_month = m.month_int
