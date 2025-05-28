WITH

matches_per_team AS (
	SELECT * FROM {{ ref('int_football_data__matches_per_team') }}
),

matches AS (
	SELECT * FROM {{ ref('stg_football_data__matches') }}
),

teams AS (
	SELECT * FROM {{ ref('stg_football_data__teams') }} 
),

competitions AS (
	SELECT * FROM {{ ref('stg_football_data__competitions') }}
),

matches_per_team_with_matches_info AS (
	SELECT
		matches_per_team.match_id,
		matches_per_team.team_id,
		matches_per_team.team_playing_location,
		matches.competition_id,
		matches.season_id,
		matches.match_at AS match_start_at,
		matches.match_at + INTERVAL 120 MINUTES AS match_end_at,
		matches.full_time_home_score AS home_score,
		matches.full_time_away_score AS away_score,
		CASE
			-- home matches
			WHEN matches_per_team.team_playing_location = 'home' AND matches.full_time_home_score > matches.full_time_away_score THEN 3
			WHEN matches_per_team.team_playing_location = 'home' AND matches.full_time_home_score = matches.full_time_away_score THEN 1
			WHEN matches_per_team.team_playing_location = 'home' AND matches.full_time_home_score < matches.full_time_away_score THEN 0
			-- away matches
			WHEN matches_per_team.team_playing_location = 'away' AND matches.full_time_away_score > matches.full_time_home_score THEN 3
			WHEN matches_per_team.team_playing_location = 'away' AND matches.full_time_away_score = matches.full_time_home_score THEN 1
			WHEN matches_per_team.team_playing_location = 'away' AND matches.full_time_away_score < matches.full_time_home_score THEN 0
			ELSE NULL
		END AS match_team_pts, -- The number of points won during a match
	FROM matches_per_team
	LEFT JOIN matches USING (match_id)
	WHERE status = 'finished'
	ORDER BY match_start_at
),

placement_versions AS (
	SELECT 
		match_id,
		--  A new version is created every time a match finishes. 
        match_end_at AS version_start_at,
		LEAD(match_end_at) OVER (PARTITION BY team_id, competition_id, season_id ORDER BY match_end_at) AS version_end_at,
		*,
		
		-- Version Number of wins, draws, losses
		IF(match_team_pts = 3, 1, 0) AS version_number_of_wins,
		IF(match_team_pts = 1, 1, 0) AS version_number_of_draws,
		IF(match_team_pts = 0, 1, 0) AS version_number_of_losses,
		
		-- Version Number of home, away, total points won
		IF(team_playing_location = 'home', match_team_pts, 0) AS version_home_pts,
		IF(team_playing_location = 'away', match_team_pts, 0) AS version_away_pts,
		match_team_pts AS version_total_pts,
		
		-- Version Number of goals scored, goals conceded and total goals
		CASE
			WHEN team_playing_location = 'home' THEN home_score
			WHEN team_playing_location = 'away' THEN away_score
			ELSE NULL
		END AS version_number_of_goals_scored,
		CASE
			WHEN team_playing_location = 'home' THEN away_score
			WHEN team_playing_location = 'away' THEN home_score
			ELSE NULL
		END AS version_number_of_goals_conceded,
		home_score + away_score AS version_total_number_of_goals_in_match,
		
		-- Running Number of wins, draws, losses
		COUNT(IF(match_team_pts = 3, 'win', NULL)) OVER (
			PARTITION BY team_id, competition_id, season_id
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_number_of_wins,
		COUNT(IF(match_team_pts = 1, 'draw', NULL)) OVER (
			PARTITION BY team_id, competition_id, season_id
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_number_of_draws,
		COUNT(IF(match_team_pts = 0, 'loss', NULL)) OVER (
			PARTITION BY team_id, competition_id, season_id
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_number_of_losses,
		
		-- Running Number of home, away, total points won
		SUM(IF(team_playing_location = 'home', match_team_pts, 0)) OVER( 
			PARTITION BY team_id, competition_id, season_id
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_home_pts,
		SUM(IF(team_playing_location = 'away', match_team_pts, 0)) OVER(
			PARTITION BY team_id, competition_id, season_id
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_away_pts,
		SUM(match_team_pts) OVER(
			PARTITION BY team_id, competition_id, season_id 
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_total_pts,
		
		-- Running Number of goals scored, goals conceded and total goals
		SUM(
			CASE
				WHEN team_playing_location = 'home' THEN home_score
				WHEN team_playing_location = 'away' THEN away_score
				ELSE NULL
			END
		) OVER (
			PARTITION BY team_id, competition_id, season_id 
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_number_of_goals_scored,
		SUM(
			CASE
				WHEN team_playing_location = 'home' THEN away_score
				WHEN team_playing_location = 'away' THEN home_score
				ELSE NULL
			END
		) OVER (
			PARTITION BY team_id, competition_id, season_id 
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_number_of_goals_conceded,
		SUM(
			home_score + away_score
		) OVER (
			PARTITION BY team_id, competition_id, season_id 
			ORDER BY match_start_at
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS running_total_number_of_goals_in_matches,
	FROM matches_per_team_with_matches_info
)

SELECT 
        
    'standings_version' || '_' || md5( CAST( version_start_at || team_id || competition_id AS VARCHAR) )
 AS standings_version_id,
        placement_versions.match_id,
        placement_versions.version_start_at,
		placement_versions.version_end_at,
		placement_versions.team_id,
		teams.team_name,
		placement_versions.competition_id,
		competitions.competition_name,
		-- Version number of wins, draws, losses
		placement_versions.version_number_of_wins,
		placement_versions.version_number_of_draws,
		placement_versions.version_number_of_losses,
		-- Version number of home, away, total points won
		placement_versions.version_home_pts,
		placement_versions.version_away_pts,
		placement_versions.version_total_pts,
		-- Version number of goals scored, goals conceded and total goals
		placement_versions.version_number_of_goals_scored,
		placement_versions.version_number_of_goals_conceded,
		placement_versions.version_total_number_of_goals_in_match,
		-- Running number of wins, draws, losses
		placement_versions.running_number_of_wins,
		placement_versions.running_number_of_draws,
		placement_versions.running_number_of_losses,
		-- Running number of home, away, total points won
		placement_versions.running_home_pts,
		placement_versions.running_away_pts,
		placement_versions.running_total_pts,
		-- Running number of goals scored, goals conceded and total goals
		placement_versions.running_number_of_goals_scored,
		placement_versions.running_number_of_goals_conceded,
		placement_versions.running_total_number_of_goals_in_matches,
FROM placement_versions
LEFT JOIN teams USING (team_id)
LEFT JOIN competitions USING (competition_id)
