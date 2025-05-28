-- A streak is defined as an uninterupted sequence of losses, draws, or wins.
-- A streak starts at the beginning of the first match and ends at the end of the last streak match.

-- Je me suis cassé la tête sur le fait d'essayer de trouver un moyen d'avoir une colonne conditionnellement incrémentale,
-- quelque chose comme une for loop en python où l'on ferait i = i + 1 à une certaine condition
-- Mais l y a un truc en SQL qui est incrémental: c'est les running sums, les SUM() de window fonctions
-- Le principe, c'est de spotter les débuts de streaks avec un 1. 
-- Si les matchs suivants font partie du streak, on leur assigne 0.
-- Ensuite on fait la somme cumulative de cet index, ce qui va permettre d'ajouter 1 à chaque début de streak, et 0 si on est 
-- toujours dans la même streak.

WITH

matches_per_team AS (
    SELECT * FROM {{ ref('int_football_data__matches_per_team') }}
),

competitions AS (
	SELECT * FROM {{ ref('stg_football_data__competitions') }}
),

teams AS (
	SELECT * FROM {{ ref('stg_football_data__teams') }}
),

matches AS (
	SELECT 
		matches_per_team.competition_id,
		matches_per_team.team_id,
		matches_per_team.match_id, 
		matches_per_team.match_at,
		matches_per_team.team_result,
		competitions.competition_name,
		teams.team_name,
	FROM matches_per_team
	LEFT JOIN competitions USING (competition_id)
	LEFT JOIN teams USING (team_id)  
	WHERE match_status = 'finished'
	ORDER BY match_at
),

-- Step 1: Find streaks starts
matches_with_streak_starts AS (
	SELECT
		*,
		-- If the previous match didn't have the same result as this one, we are starting a new streak
		COALESCE(LAG(team_result) OVER (PARTITION BY team_id, competition_id ORDER BY match_at) != team_result, TRUE)  AS is_streak_start,
	FROM matches
),

streaks_matches AS (
	SELECT
		*,
		-- Since each streak start is spotted by a TRUE (meaning: the value "1"), all match of the same streaks will have the same number assigned,
		-- provided that the data is ordered and that we cumulate this number over all matches.
		SUM(is_streak_start) OVER (PARTITION BY team_id, competition_id ORDER BY match_at) AS streak_id,
		ROW_NUMBER() OVER(PARTITION BY team_id, competition_id ORDER BY match_at DESC) = 1 AS is_team_last_match, -- Flag created in order to spot currently ongoing streaks		
	FROM matches_with_streak_starts
)

SELECT
	{{ surrogate_key('streak', 'competition_id || team_id || streak_id') }} AS streak_id,
	competition_id,
	competition_name,
	team_id,
	team_name,
	ARRAY_AGG(match_id) AS streak_match_ids,
	ARRAY_AGG(match_at) AS streak_match_ats,
	COUNT(match_id) AS streak_number_of_matches,
	MIN(team_result) AS streak_type,
	MIN(match_id) AS streak_start_match_id,
	MAX(match_id) AS streak_end_match_id,
	MIN(match_at) AS streak_start_at,
	-- streak_end_at shoud be NULL if the streak is ongoing. 
	-- Otherwise, the streak ends at the end of the last match of the streak
	IF(
		MAX(is_team_last_match) = 1, -- For more details on this line, please read point (1) of the model's documention
		NULL,
		MAX(match_at) + INTERVAL 120 MINUTES 
	) AS streak_end_at, -- Note that the streak_end_at is inclusive of the last streak match
FROM streaks_matches
GROUP BY streak_id, team_id, team_name, competition_id, competition_name
