WITH

teams AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__teams') }}
), 

deduplicated_teams AS (
    SELECT
        *
    FROM teams
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id) = 1
),

running_competitions AS (
    -- In the 'runningCompetitions' Array of Structs, we want to only keep id column, not all columns
    -- To do so, we unnest, select the id and then re-agregate into an array
    SELECT
        id,
        ARRAY_AGG(running_competition_id) AS running_competitions_ids,
    FROM (
        SELECT
            id,
            CAST(UNNEST(runningCompetitions).id AS VARCHAR) AS running_competition_id,
        FROM deduplicated_teams
    )
    GROUP BY 1
),

players AS (
    -- In the 'squad' Array of Structs, only keep id column, not all columns
    SELECT
        id, 
        ARRAY_AGG(player_id) AS player_ids
    FROM (
        SELECT
            id,
            CAST(UNNEST(squad).id AS VARCHAR) AS player_id
        FROM deduplicated_teams
    )
    GROUP BY 1
),

casted_properties AS (
    SELECT
        CAST(deduplicated_teams.id AS VARCHAR) AS team_id,
        CAST(deduplicated_teams.name AS VARCHAR) AS team_name,
        CAST(deduplicated_teams.shortName AS VARCHAR) AS team_short_name,
        CAST(deduplicated_teams.tla AS VARCHAR) AS team_code,
        CAST(deduplicated_teams.area.name AS VARCHAR) AS country_name,
        CAST(deduplicated_teams.address AS VARCHAR) AS team_address,
        CAST(deduplicated_teams.website AS VARCHAR) AS website_url,
        CASE 
            -- Dealing with missing data (the foundation dates were found on the internet)
            WHEN deduplicated_teams.founded IS NULL AND team_name = 'Sheffield United FC' THEN CAST('1889-01-01' AS DATE)
            WHEN deduplicated_teams.founded IS NULL AND team_name = 'AVS' THEN CAST('2023-01-01' AS DATE)
            WHEN deduplicated_teams.founded IS NULL AND team_name = 'CF Estrela da Amadora' THEN CAST('2020-01-01' AS DATE)
            WHEN deduplicated_teams.founded IS NOT NULL THEN CAST(CONCAT(CAST(CAST(founded AS INTEGER) AS VARCHAR), '-01-01') AS DATE)
        END AS foundation_year_date, -- If the year of foundation is NULL, an error pops out because NULL cannot be converted into DATE
        STRING_SPLIT(CAST(deduplicated_teams.clubColors AS VARCHAR), ' / ') AS team_colors,
        CAST(deduplicated_teams.venue AS VARCHAR) AS arena_name,
        CAST(deduplicated_teams.competition.id AS VARCHAR) AS main_competition_id,
        running_competitions.running_competitions_ids, -- Already casted in earlier CTE
        players.player_ids, -- Already casted in earlier CTE
        CAST(deduplicated_teams.coach.id AS VARCHAR) AS coach_id,
        IF(
            deduplicated_teams.coach.contract.start IS NOT NULL, 
            CAST(CONCAT(deduplicated_teams.coach.contract.start, '-01') AS DATE), -- We treat each date provided by the football data API as is a first day of month, due to lack of precision in the API
            NULL
        ) AS coach_contract_start_date,
        IF(
            deduplicated_teams.coach.contract.until IS NOT NULL,
            CAST(CONCAT(deduplicated_teams.coach.contract.until, '-01') AS DATE),  -- Same as previous comment
            NULL
        ) AS coach_contract_end_date,
        CAST(deduplicated_teams.season.id AS VARCHAR) AS season_id,
    FROM deduplicated_teams
    LEFT JOIN running_competitions USING (id)
    LEFT JOIN players USING (id)
)

SELECT
    team_id,
    team_name,
    team_short_name,
    team_code,
    country_name,
    team_address,
    website_url,
    foundation_year_date,
    team_colors,
    arena_name,
    main_competition_id,
    running_competitions_ids,
    player_ids,
    coach_id,
    coach_contract_start_date,
    coach_contract_end_date,
    season_id
FROM casted_properties
