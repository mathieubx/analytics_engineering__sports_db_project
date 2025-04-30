WITH

teams AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__teams') }}
)

SELECT
    CAST(UNNEST(squad).id AS VARCHAR) AS player_id,
    CAST(UNNEST(squad).name AS VARCHAR) AS player_name,
    CAST(id AS VARCHAR) AS team_id,
    CAST(UNNEST(squad).position AS VARCHAR) AS position,
    CAST(UNNEST(squad).dateofbirth AS DATE) AS birth_date,
    CAST(UNNEST(squad).nationality AS VARCHAR) AS country,
FROM teams
