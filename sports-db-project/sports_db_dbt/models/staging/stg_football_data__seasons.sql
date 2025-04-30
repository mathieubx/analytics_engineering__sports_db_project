WITH

teams AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__teams') }}
),

seasons AS (
    SELECT DISTINCT
        CAST(season.id AS VARCHAR) AS season_id,
        CAST(season.startDate AS DATE) AS start_date,
        CAST(season.endDate AS DATE) AS end_date,
        CAST(competition.id AS VARCHAR) AS competition_id,
        CAST(competition.name AS VARCHAR) AS competition_name,
    FROM teams
)

SELECT
    -- For a better clarity in season_ids, I'll craft a new surrogate key
    strip_accents(REPLACE(LOWER(competition_name), ' ', '_')) || '_' || EXTRACT(YEAR FROM start_date) || '_' || EXTRACT(YEAR FROM end_date) AS season_uid,
    season_id,
    start_date,
    end_date,
    competition_id
FROM seasons
