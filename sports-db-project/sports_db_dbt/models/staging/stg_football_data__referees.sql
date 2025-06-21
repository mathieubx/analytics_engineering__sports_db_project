WITH

matches AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__matches') }}
)

SELECT DISTINCT
    CAST(UNNEST(referees).id AS VARCHAR) AS referee_id,
    CAST(UNNEST(referees).name AS VARCHAR) AS full_name,
    CASE 
        WHEN UNNEST(referees).id = 167782 THEN 'Israel'
        WHEN UNNEST(referees).id = 80747 THEN 'Spain'
        WHEN UNNEST(referees).id = 176741 THEN 'Portugal'
        ELSE CAST(UNNEST(referees).nationality AS VARCHAR)
    END AS nationality,
FROM matches
