WITH

teams AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__teams') }}
)

SELECT
    CAST(coach.id AS VARCHAR) AS coach_id,
    CAST(coach.firstname AS VARCHAR) AS first_name,
    CAST(coach.lastname AS VARCHAR) AS last_name,
    CAST(coach.name AS VARCHAR) AS full_name,
    CAST(coach.dateofbirth AS DATE) AS birth_date,
    CAST(coach.nationality AS VARCHAR) AS nationality
FROM teams
WHERE id IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY id) = 1 -- Deduplicating
