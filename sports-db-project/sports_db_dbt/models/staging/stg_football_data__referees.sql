WITH

matches AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__matches') }}
)

SELECT DISTINCT
    CAST(UNNEST(referees).id AS VARCHAR) AS referee_id,
    CAST(UNNEST(referees).name AS VARCHAR) AS full_name,
    CAST(UNNEST(referees).nationality AS VARCHAR) AS nationality,
FROM matches
