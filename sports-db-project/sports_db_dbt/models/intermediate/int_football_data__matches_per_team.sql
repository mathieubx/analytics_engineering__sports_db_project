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

home_and_away_matches AS (
    SELECT
        *
    FROM home_matches
    UNION ALL
    SELECT 
        *
    FROM away_matches
),

matches_per_team AS (
    SELECT DISTINCT -- Filter duplicates from source
        {{ surrogate_key('match', 'home_and_away_matches.team_id || home_and_away_matches.match_id') }} AS team_match_id,
        home_and_away_matches.match_id,
        home_and_away_matches.competition_id,
        home_and_away_matches.team_id,
        home_and_away_matches.team_playing_location,
        home_and_away_matches.status AS match_status,
        CASE 
		    WHEN home_and_away_matches.team_playing_location = matches.winner THEN 'win'
		    WHEN matches.winner = 'draw' THEN 'draw'
		    WHEN home_and_away_matches.team_playing_location != matches.winner THEN 'loss'
		    ELSE NULL
	    END AS team_result,
        home_and_away_matches.match_at,
    FROM home_and_away_matches
    LEFT JOIN matches USING (match_id)
)

SELECT
    team_match_id,
    match_id,
    competition_id,
    team_id,
    team_playing_location,
    match_at,
    match_status,
    team_result,
FROM matches_per_team
