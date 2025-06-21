WITH 

players AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__players') }}
)

SELECT 
	CAST(id AS VARCHAR) AS player_id,
	CAST(name AS VARCHAR) AS name,
	CAST(firstName AS VARCHAR) AS first_name,
	CAST(lastName AS VARCHAR) AS last_name,
	CASE 
		WHEN id = 191106 THEN CAST('2005-01-04' AS DATE)
		WHEN id = 191127 THEN CAST('1998-05-12' AS DATE)
		WHEN id = 263454 THEN CAST('2007-05-09' AS DATE)
		WHEN id = 170381 THEN CAST('1993-02-19' AS DATE)
		WHEN id = 160790 THEN CAST('2004-04-06' AS DATE)
		WHEN id = 191811 THEN {{ var('default_birth_date') }} -- Unknown so we set a default date
		ELSE CAST(dateOfBirth AS DATE) 
	END AS date_of_birth,
	CASE
		-- Attributing missing nationalities
		WHEN id = 192652 THEN 'Brazil'
		-- Renaming nationalities
		WHEN nationality = 'UA Emirates' THEN 'United Arab Emirates'
		WHEN nationality = 'DR Congo' THEN 'Congo DR'
		WHEN nationality = 'Ivory Coast' THEN 'Côte d’Ivoire'
		WHEN nationality = 'São Tomé and Príncipe' THEN 'São Tomé e Príncipe'
		WHEN nationality = 'USA' THEN 'United States'
		ELSE CAST(nationality AS VARCHAR)
	END AS nationality,
	CAST(section AS VARCHAR) AS section,
	CAST(position AS VARCHAR) AS position,
	CAST(shirtNumber AS INTEGER) AS player_number,
	CAST(current_team_id AS VARCHAR) AS current_team_id,
	DATE_TRUNC('MONTH', CAST(player_contract.start || '-01' AS DATE)) AS contract_start_month,
	DATE_TRUNC('MONTH', CAST(player_contract.until || '-01' AS DATE)) AS contract_end_month,
	CAST(lastUpdated AS DATETIME) AS last_updated_at,
FROM players
