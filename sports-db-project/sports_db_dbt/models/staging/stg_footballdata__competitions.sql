WITH

competitions AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__competitions') }}
)

SELECT
    {{ surrogate_key('competition', 'name') }} AS competition_id,
    CAST(id AS VARCHAR) AS football_data_competition_id,
    CAST(name AS VARCHAR) AS competition_name,
    CAST(code AS VARCHAR) AS competition_code,  
    LOWER(CAST(type AS VARCHAR)) AS competition_type,
    CAST(area.name AS VARCHAR) AS country_name,
    CAST(currentSeason.id AS VARCHAR) AS current_season_id, 
FROM competitions
