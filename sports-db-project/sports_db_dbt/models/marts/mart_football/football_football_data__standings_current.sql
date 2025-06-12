-- Strategy -> Split ALL the matches in 2 categories: Home matches, and Away matches. All stats of a team are the sum of the stats at home, and away. And it's easier to compute.
-- -> We build at CTE for Home matches, and another one for Away matches.
-- -> We compute all the stats of interest for each team at home, and each team away.
-- -> To get global stats, we sum the home and the away stats

WITH 

matches AS (
	SELECT * FROM {{ ref("stg_football_data__matches") }}
),

teams AS (
	SELECT * FROM {{ ref("stg_football_data__teams") }}
),

competitions AS (
	SELECT * FROM {{ ref("stg_football_data__competitions") }}
),

matches_with_pts AS (
	SELECT
		CASE
			WHEN winner = 'home' THEN 3
			WHEN winner = 'away' THEN 0
			WHEN winner = 'draw' THEN 1
			ELSE NULL
		END AS home_team_pts,
			CASE
			WHEN winner = 'home' THEN 0
			WHEN winner = 'away' THEN 3
			WHEN winner = 'draw' THEN 1
			ELSE NULL
		END AS away_team_pts,
		*
	FROM matches
    WHERE status IN ('finished', 'awarded') AND stage IN ('regular season', 'league stage') -- Don't account for playoffs (for instance in CL)
	-- Note that the 'awarded' status corresponds to a decision extern to the match setting the result. Let's say your fans burned the stadium, a commission could decide to award a 2-0 win to your opponent. 
),	

home_matches AS (
	SELECT
		home_team_id AS team_id,
		competition_id AS competition_id,
		SUM(home_team_pts) AS sum_home_pts,
		SUM(full_time_home_score) AS sum_home_goals_scored,
		SUM(full_time_away_score) AS sum_home_goals_conceded, 
		COUNT(home_team_id) AS number_of_home_matches,
		COUNT(IF(full_time_home_score > full_time_away_score, 'home win', NULL)) AS number_of_home_wins,
		COUNT(IF(full_time_home_score = full_time_away_score, 'home draw', NULL)) AS number_of_home_draws,
		COUNT(IF(full_time_home_score < full_time_away_score, 'home loss', NULL)) AS number_of_home_losses,
	FROM matches_with_pts
	GROUP BY competition_id, home_team_id
),

away_matches AS (
	SELECT
		away_team_id AS team_id,
		competition_id AS competition_id,
		SUM(away_team_pts) AS sum_away_pts,
		SUM(full_time_away_score) AS sum_away_goals_scored,
		SUM(full_time_home_score) AS sum_away_goals_conceded,
		COUNT(away_team_id) AS number_of_away_matches,
		COUNT(IF(full_time_away_score > full_time_home_score, 'away win', NULL)) AS number_of_away_wins,
		COUNT(IF(full_time_away_score = full_time_home_score, 'away draw', NULL)) AS number_of_away_draws,
		COUNT(IF(full_time_away_score < full_time_home_score, 'away loss', NULL)) AS number_of_away_losses,
	FROM matches_with_pts
	GROUP BY competition_id, away_team_id
),

current_standings AS (
	SELECT
		home_matches.team_id,
		home_matches.competition_id,
		home_matches.sum_home_pts AS home_pts,
		away_matches.sum_away_pts AS away_pts,
		home_matches.sum_home_pts + away_matches.sum_away_pts AS total_pts,	
		home_matches.number_of_home_matches,
		away_matches.number_of_away_matches,
		home_matches.number_of_home_matches + away_matches.number_of_away_matches AS total_number_of_matches,
		home_matches.sum_home_goals_scored AS goals_scored_at_home,
		home_matches.sum_home_goals_conceded AS goals_conceded_at_home,
		away_matches.sum_away_goals_scored AS goals_scored_away,
		away_matches.sum_away_goals_conceded AS goals_conceded_away,
		home_matches.sum_home_goals_scored + away_matches.sum_away_goals_scored AS total_goals_scored,
		home_matches.sum_home_goals_conceded + away_matches.sum_away_goals_conceded AS total_goals_conceded,
		home_matches.number_of_home_wins,
		home_matches.number_of_home_draws,
		home_matches.number_of_home_losses,
		away_matches.number_of_away_wins,
		away_matches.number_of_away_draws,
		away_matches.number_of_away_losses,
		home_matches.number_of_home_wins + away_matches.number_of_away_wins AS number_of_wins,
		home_matches.number_of_home_draws + away_matches.number_of_away_draws AS number_of_draws,
		home_matches.number_of_home_losses + away_matches.number_of_away_losses AS number_of_losses,
	FROM home_matches
	LEFT JOIN away_matches USING (team_id, competition_id)
)

SELECT
	-- Ensuring the unicity of the key by creating a surrogate key from team_id/competition_id combination
	{{ surrogate_key('current_placement', 'current_standings.team_id || current_standings.competition_id') }} AS current_placement_id,
    ROW_NUMBER() OVER(
        PARTITION BY current_standings.competition_id 
        -- The criteria to determine ranking in most leagues are points, then goal differential, then goals scored
        ORDER BY 
            current_standings.total_pts DESC, -- Points
	        (current_standings.total_goals_scored - current_standings.total_goals_conceded) DESC, -- Goal differential
	        current_standings.total_goals_scored DESC -- Goals scored
    ) AS ranking,
	competitions.competition_name,
    current_standings.competition_id,
    teams.team_name,
	teams.logo_url AS team_logo_url,
    current_standings.team_id,
    current_standings.home_pts,
    current_standings.away_pts,
    current_standings.total_pts,
    current_standings.number_of_home_matches,
    current_standings.number_of_away_matches,
    current_standings.total_number_of_matches,
    current_standings.goals_scored_at_home,
    current_standings.goals_conceded_at_home,
    current_standings.goals_scored_away,
    current_standings.goals_conceded_away,
    current_standings.total_goals_scored,
    current_standings.total_goals_conceded,
	current_standings.number_of_home_wins,
	current_standings.number_of_home_draws,
	current_standings.number_of_home_losses,
	current_standings.number_of_away_wins,
	current_standings.number_of_away_draws,
	current_standings.number_of_away_losses,
	current_standings.number_of_wins,
	current_standings.number_of_draws,
	current_standings.number_of_losses,
FROM current_standings
LEFT JOIN teams ON current_standings.team_id = teams.team_id
LEFT JOIN competitions ON current_standings.competition_id = competitions.competition_id
