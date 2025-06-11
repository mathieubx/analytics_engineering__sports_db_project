WITH

champions_league_matches AS (
	SELECT * FROM {{ ref('stg_football_data__matches') }} WHERE competition_id = '2001'
),

competitions AS (
	SELECT * FROM {{ ref('stg_football_data__competitions') }}
),

teams AS (
	SELECT * FROM {{ ref('stg_football_data__teams') }}
),

stage_orders AS (
	SELECT 
		'playoffs' AS stage,
		50 AS stage_order,
	UNION ALL
	SELECT 
		'last 16' AS stage,
		40 AS stage_order,
	UNION ALL
	SELECT 
		'quarter finals' AS stage,
		30 AS stage_order,
	UNION ALL
	SELECT 
		'semi finals' AS stage,
		20 AS stage_order,
	UNION ALL
	SELECT 
		'final' AS stage,
		10 AS stage_order,
)

SELECT DISTINCT
	champions_league_matches.match_id,
    champions_league_matches.match_at,
    champions_league_matches.status,
    champions_league_matches.matchday_number,
    champions_league_matches.stage AS match_stage,
    champions_league_matches.home_team_id,
	home_teams.team_name AS home_team_name,
    champions_league_matches.away_team_id,
	away_teams.team_name AS away_team_name,
    champions_league_matches.referees_ids,
    champions_league_matches.winner,
    champions_league_matches.duration_type,
    champions_league_matches.full_time_home_score,
    champions_league_matches.full_time_away_score,
    champions_league_matches.half_time_home_score,
    champions_league_matches.half_time_away_score,
    champions_league_matches.area_id,
    champions_league_matches.competition_id,
	competitions.competition_name,
    champions_league_matches.season_id,
    champions_league_matches.last_updated_at,
	stage_orders.stage_order,
FROM champions_league_matches
LEFT JOIN stage_orders USING (stage)
LEFT JOIN competitions USING (competition_id)
LEFT JOIN teams AS home_teams ON champions_league_matches.home_team_id = home_teams.team_id
LEFT JOIN teams AS away_teams ON champions_league_matches.away_team_id = away_teams.team_id
WHERE stage != 'league stage'
ORDER BY stage_order 
