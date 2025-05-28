-- Testing that the individual dates contained in the `streak_match_ats` column of football_football_data__streaks all are after now
WITH 

unnested_streaks AS (
    SELECT 
        * EXCLUDE (streak_match_ats),
        UNNEST(streak_match_ats) AS streak_match_at, 
    FROM {{ ref('football_football_data__streaks') }}
)

SELECT 
    *
FROM unnested_streaks
WHERE streak_match_at > current_date