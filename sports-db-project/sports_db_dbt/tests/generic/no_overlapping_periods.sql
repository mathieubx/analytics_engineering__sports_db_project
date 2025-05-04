{% test no_overlapping_periods(model, start_at, end_at, id_column) %}

WITH 

periods_1 AS (
	SELECT * FROM {{ model }}
),

periods_2 AS (
	SELECT * FROM {{ model }}
)

SELECT 
		*
	FROM periods_1
	INNER JOIN periods_2 ON periods_1.{{ start_at }} < periods_2.{{ end_at }}
		AND periods_1.{{ end_at }} > periods_2.{{ start_at }}
		AND periods_1.{{ id_column }} != periods_2.{{ id_column }} -- Preventing self-joins
		-- Comparing periods that have the same team_id and competition_id (kind of duh, but I forgot the condition at first, creating massive amounts of duplicates)
		AND periods_1.team_id = periods_2.team_id
		AND periods_1.competition_id = periods_2.competition_id

{% endtest %}