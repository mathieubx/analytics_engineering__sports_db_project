WITH 

matches_per_team AS (
    SELECT * FROM {{ ref('int_football_data__matches_per_team') }}
),

match_scores AS (
    SELECT DISTINCT 
        match_id,
        full_time_home_score AS home_score,
        full_time_away_score AS away_score,
    FROM {{ ref('stg_football_data__matches') }}
), 

teams AS (
    SELECT * FROM {{ ref('stg_football_data__teams') }}
),

competitions AS (
    SELECT * FROM {{ ref('stg_football_data__competitions') }}
), 

matches AS (
    -- Computing the number of points won, the number of goals scored, and the number of goals conceded by the team 
    SELECT
        *,
        teams.team_name,
        competitions.competition_name,
        CAST(DATE_TRUNC('MONTH', matches_per_team.match_at) AS TIMESTAMP) AS match_month,
        CASE
            WHEN matches_per_team.team_playing_location = 'home' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'win' THEN 3
            WHEN matches_per_team.team_playing_location = 'home' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'draw' THEN 1
            WHEN matches_per_team.team_playing_location = 'home' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'loss' THEN 0
            ELSE 0.
        END AS home_pts,
        CASE
            WHEN matches_per_team.team_playing_location = 'away' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'win' THEN 3
            WHEN matches_per_team.team_playing_location = 'away' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'draw' THEN 1
            WHEN matches_per_team.team_playing_location = 'away' AND matches_per_team.match_status = 'finished' AND matches_per_team.team_result = 'loss' THEN 0
            ELSE 0
        END AS away_pts,
        CASE
            WHEN matches_per_team.team_result = 'win' AND matches_per_team.match_status = 'finished' THEN 3
            WHEN matches_per_team.team_result = 'draw' AND matches_per_team.match_status = 'finished' THEN 1
            WHEN matches_per_team.team_result = 'loss' AND matches_per_team.match_status = 'finished' THEN 0
            ELSE NULL
        END AS total_pts,
        IF(
            matches_per_team.team_playing_location = 'home',
            match_scores.home_score,
            match_scores.away_score
        ) AS goals_scored,
        IF(
            matches_per_team.team_playing_location = 'home',
            match_scores.away_score,
            match_scores.home_score
        ) AS goals_conceded,
    FROM matches_per_team
    LEFT JOIN match_scores USING (match_id)
    LEFT JOIN teams USING (team_id)
    LEFT JOIN competitions USING (competition_id)
),

matches_with_opponent_info AS (
    -- There are 2 rows for each match in matches_per_team. One for each team. This CTE finds the other row.
    -- The join looks at the matches per team, and returns row that have the same match_id, but not the same team_id
    SELECT
        team_match.team_match_id,
        team_match.match_id,
        opponent_team_match.team_id AS opponent_team_id,
        opponent_teams.team_name AS opponent_team_name
    FROM matches_per_team AS team_match
    LEFT JOIN matches_per_team AS opponent_team_match 
        ON team_match.match_id = opponent_team_match.match_id 
            AND team_match.team_id != opponent_team_match.team_id
    -- Then we add the team names
    LEFT JOIN teams AS opponent_teams ON opponent_team_match.team_id = opponent_teams.team_id  
)

SELECT 
    matches.team_match_id,
    matches.match_id,
    matches.competition_id,
    matches.competition_name,
    matches.team_id,
    matches.team_name,
    matches.team_playing_location,
    matches.match_at,
    matches.match_month,
    matches.team_result,
    matches.match_status,
    matches.match_stage,
    matches_with_opponent_info.opponent_team_id,
    matches_with_opponent_info.opponent_team_name,
    CAST(matches.home_pts AS INTEGER) AS home_pts, -- Forced to CAST, otherwise this column is returned as a DECIMAL 
    CAST(matches.away_pts AS INTEGER) AS away_pts,
    CAST(matches.total_pts AS INTEGER) AS total_pts,
    matches.goals_scored,
    matches.goals_conceded,
FROM matches
LEFT JOIN matches_with_opponent_info USING (team_match_id)
