WITH

matches AS (
    SELECT * FROM {{ ref('stg_football_data__matches') }}
),

home_matches AS (
    SELECT
        * EXCLUDE (home_team_id, away_team_id),
        home_team_id AS team_id,
        'home' AS team_playing_location,
    FROM matches
),

away_matches AS (
    SELECT
        * EXCLUDE (home_team_id, away_team_id),
        away_team_id AS team_id,
        'away' AS team_playing_location
    FROM matches
), 

matches_per_team AS (
    SELECT
        *
    FROM home_matches
    UNION ALL
    SELECT 
        *
    FROM away_matches
)

SELECT DISTINCT
    {{ surrogate_key('match', 'team_id || match_id') }} AS team_match_id,
    match_id,
    team_id,
    team_playing_location,
FROM matches_per_team
