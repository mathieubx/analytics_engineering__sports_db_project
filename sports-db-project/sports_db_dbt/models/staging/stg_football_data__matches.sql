WITH 

matches AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__matches') }}
),

filtered_matches AS (
    -- This table is a current view. 
    -- The source contains South American matches from the previous season that need to be filtered out 
    SELECT 
        *
    FROM matches
    WHERE NOT (
        competition.id IN (2152, 2013) -- Ids of Copa Libertadores and Brazilian Serié A 
        AND EXTRACT ( YEAR FROM (CAST(utcDate AS DATE)) ) == EXTRACT(YEAR FROM current_date) - 1 -- Matches in 2024
    )
),

referees_ids AS (
    SELECT
        match_id,
        ARRAY_AGG(referee_id) AS referees_ids
    FROM (
        SELECT
            CAST(id AS VARCHAR) AS match_id, 
            UNNEST(referees).id AS referee_id, 
        FROM filtered_matches
    )
    GROUP BY 1    
), 

casted_properties AS (
    SELECT
        CAST(filtered_matches.id AS VARCHAR) AS match_id,
        CAST(filtered_matches.utcDate AS TIMESTAMP) AS match_at,
        REPLACE(LOWER(CAST(filtered_matches.status AS VARCHAR)), '_', ' ') AS status,
        CAST(filtered_matches.matchday AS INTEGER) AS matchday_number,
        REPLACE(LOWER(CAST(filtered_matches.stage AS VARCHAR)), '_', ' ') AS stage,
        CAST(filtered_matches.homeTeam.id AS VARCHAR) AS home_team_id,
        CAST(filtered_matches.awayTeam.id AS VARCHAR) AS away_team_id,
        referees_ids.referees_ids,
        REPLACE(LOWER(CAST(filtered_matches.winner AS VARCHAR)),'_', ' ') AS winner,
        LOWER(CAST(filtered_matches.duration AS VARCHAR)) AS duration_type,
        CAST(filtered_matches.full_time.home AS INTEGER) AS full_time_home_score,
        CAST(filtered_matches.full_time.away AS INTEGER) AS full_time_away_score,
        CAST(filtered_matches.half_time.home AS INTEGER) AS half_time_home_score,
        CAST(filtered_matches.half_time.away AS INTEGER) AS half_time_away_score,
        CAST(filtered_matches.area.id AS VARCHAR) AS area_id,
        CAST(filtered_matches.competition.id AS VARCHAR) AS competition_id,
        CAST(filtered_matches.season.id AS VARCHAR) AS season_id,
        CAST(filtered_matches.lastUpdated AS TIMESTAMP) AS last_updated_at
    FROM filtered_matches
    LEFT JOIN referees_ids ON filtered_matches.id = referees_ids.match_id
)

SELECT
    match_id,
    match_at,
    status,
    matchday_number,
    stage,
    home_team_id,
    away_team_id,
    referees_ids,
    winner,
    duration_type,
    full_time_home_score,
    full_time_away_score,
    half_time_home_score,
    half_time_away_score,
    area_id,
    competition_id,
    season_id,
    last_updated_at
FROM casted_properties
