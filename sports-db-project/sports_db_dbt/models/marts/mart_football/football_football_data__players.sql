WITH 

players AS (
    SELECT * FROM {{ ref('stg_football_data__players') }}
),

teams AS (
    SELECT * FROM {{ ref('stg_football_data__teams') }}
),

competitions AS (
	SELECT * FROM {{ ref('stg_football_data__competitions') }}
),

players_main_competitions AS (
    SELECT
        players.*,
        teams.team_name AS team_name,
        teams.main_competition_id AS competition_id,
        competitions.competition_name,
        DATE_SUB('year', date_of_birth, current_date) AS age,
    FROM players
    LEFT JOIN teams ON players.current_team_id = teams.team_id
    LEFT JOIN competitions ON teams.main_competition_id = competitions.competition_id    
)

SELECT 
	player_id,
	name,
	date_of_birth,
	age,
	nationality,
	section,
	position,
	player_number,
	current_team_id AS team_id,
	team_name,
	competition_id,
	competition_name,
	contract_start_month,
	contract_end_month,
FROM players_main_competitions