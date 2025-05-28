WITH 

players AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__players') }}
)

SELECT 
	CAST(id AS VARCHAR) AS player_id,
	CAST(name AS VARCHAR) AS name,
	CAST(firstName AS VARCHAR) AS first_name,
	CAST(lastName AS VARCHAR) AS last_name,
	CAST(dateOfBirth AS DATE) AS date_of_birth,
	CAST(nationality AS VARCHAR) AS nationality,
	CAST(section AS VARCHAR) AS section,
	CAST(position AS VARCHAR) AS position,
	CAST(shirtNumber AS INTEGER) AS player_number,
	CAST(current_team_id AS VARCHAR) AS current_team_id,
	DATE_TRUNC('MONTH', CAST(player_contract.start || '-01' AS DATE)) AS contract_start_month,
	DATE_TRUNC('MONTH', CAST(player_contract.until || '-01' AS DATE)) AS contract_end_month,
	CAST(lastUpdated AS DATETIME) AS last_updated_at,
FROM players
