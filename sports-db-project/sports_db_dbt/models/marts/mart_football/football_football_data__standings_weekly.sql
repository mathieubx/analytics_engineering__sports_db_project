-- Versions at the beginning of the week 

WITH 

standings_versions AS (
    -- The end date of the weekly standings is a bit tricky. We need to provide an end date to the current version so that it appears in the final result
    -- Because the join wants the week_start_date to be **within** the start_date and end_date of a version.
    SELECT 
    	* EXCLUDE(version_end_at),
	    IF(
        -- First, we spot the current version
	        ROW_NUMBER() OVER(
	                PARTITION BY team_id, competition_id, season_id
	                ORDER BY version_start_at DESC
	        ) = 1,
	        -- If it's the current version, we simply add 1 week as an artificial version_end_at
	        version_start_at + INTERVAL 1 WEEK,
	        -- Else:
	        version_end_at
	     ) AS version_end_at
    FROM {{ ref('football_football_data__standings_versions') }}
),

calendar AS (
    SELECT * FROM {{ ref('utils__calendar') }}
),

-- First, start from the beginning of a week
weekly_calendar AS (
    SELECT DISTINCT
        week_start_date
    FROM calendar
)

-- Join the versions and add ranking per week. This step is straightforward since we only have one version per week.
-- It would have been more complex if we had multiple versions per week.
SELECT 
    weekly_calendar.week_start_date,
    standings_versions.standings_version_id,
    standings_versions.match_id,
    standings_versions.version_start_at,
    standings_versions.version_end_at,
    standings_versions.team_id,
    standings_versions.team_name,
    standings_versions.competition_id,
    standings_versions.competition_name,
    standings_versions.season_id,
    standings_versions.season_name,
    -- Version number of wins, draws, losses
    standings_versions.version_number_of_wins,
    standings_versions.version_number_of_draws,
    standings_versions.version_number_of_losses,
    -- Version number of home, away, total points won
    standings_versions.version_home_pts,
    standings_versions.version_away_pts,
    standings_versions.version_total_pts,
    -- Version number of goals scored, goals conceded and total goals
    standings_versions.version_number_of_goals_scored,
    standings_versions.version_number_of_goals_conceded,
    standings_versions.version_total_number_of_goals_in_match,
    -- Running number of wins, draws, losses
    standings_versions.running_number_of_wins,
    standings_versions.running_number_of_draws,
    standings_versions.running_number_of_losses,
    -- Running number of home, away, total points won
    standings_versions.running_home_pts,
    standings_versions.running_away_pts,
    standings_versions.running_total_pts,
    -- Running number of goals scored, goals conceded and total goals
    standings_versions.running_number_of_goals_scored,
    standings_versions.running_number_of_goals_conceded,
    standings_versions.running_total_number_of_goals_in_matches,
	-- Running number of goals scored and conceded at home, and away
	standings_versions.running_number_of_goals_scored_at_home,
	standings_versions.running_number_of_goals_scored_away,
	standings_versions.running_number_of_goals_conceded_at_home,
	standings_versions.running_number_of_goals_conceded_away,
    -- Added columns
    ROW_NUMBER() 
        OVER (
            PARTITION BY weekly_calendar.week_start_date, standings_versions.competition_id 
            ORDER BY standings_versions.running_total_pts DESC, -- Points
    	        (standings_versions.running_number_of_goals_scored - standings_versions.running_number_of_goals_conceded) DESC, -- Goal differential
    	        standings_versions.running_number_of_goals_scored DESC -- Goals scored
    ) AS ranking,
FROM weekly_calendar
LEFT JOIN standings_versions ON weekly_calendar.week_start_date >= standings_versions.version_start_at 
            AND weekly_calendar.week_start_date < standings_versions.version_end_at
ORDER BY 1 DESC
