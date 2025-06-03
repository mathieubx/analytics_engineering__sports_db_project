WITH

seasons AS (
	SELECT * FROM dev.stg_football_data__seasons
),

competitions AS (
	SELECT * FROM dev.stg_football_data__competitions
)

SELECT 
	seasons.season_id,
	seasons.start_date,
	seasons.end_date,
	seasons.competition_id,
	competitions.competition_name,
	IF(
		competition_id = 2013, -- Brazil league has its seasons over one single year, contrary to european leagues
		competitions.competition_name || ' ' || seasons.start_date,
		competitions.competition_name || ' ' || EXTRACT(YEAR FROM seasons.start_date) || ' - ' || EXTRACT(YEAR FROM seasons.end_date)
	) AS season_name,
FROM seasons
LEFT JOIN competitions USING (competition_id)
