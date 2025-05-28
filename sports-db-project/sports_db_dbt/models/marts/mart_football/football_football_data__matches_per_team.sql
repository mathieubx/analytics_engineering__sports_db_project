WITH 

matches_per_team AS (
    SELECT * FROM {{ ref('int_football_data__matches_per_team') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_football_data__teams') }}
),

competitions AS (
    SELECT * FROM {{ ref('stg_football_data__competitions') }}
)

SELECT 
    matches.team_match_id,
    matches.match_id,
    matches.competition_id,
    competitions.competition_name,
    matches.team_id,
    teams.team_name,
    matches.team_playing_location,
    matches.match_at,
    CAST(DATE_TRUNC('MONTH', matches.match_at) AS TIMESTAMP) AS match_month,
    matches.team_result,
    matches.match_status,
    matches_with_opponent_info.team_id AS opponent_team_id
FROM matches_per_team AS matches
LEFT JOIN teams USING (team_id)
LEFT JOIN competitions USING (competition_id)
-- This join looks at the matches per team, and returns row that have the same match_id, but not the same team_id
-- There are 2 rows for each match in matches_per_team. One for each team. This join finds the other row.
LEFT JOIN matches_per_team AS matches_with_opponent_info 
    ON matches.match_id = matches_with_opponent_info.match_id 
    AND matches.team_id != matches_with_opponent_info.team_id
ORDER BY 2