WITH

areas AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__areas') }}
)

SELECT
    {{ surrogate_key('country', 'name') }} AS country_id,
    CAST(id AS VARCHAR) AS area_id,
    CASE
        WHEN id = 2030 THEN 'Bosnia-Herzegovina'
        WHEN id = 2192 THEN 'Ireland'
        WHEN id = 2083 THEN 'North Macedonia'
        WHEN id = 2123 THEN 'South Korea'
        ELSE CAST(name AS VARCHAR) 
    END AS country_name,
    CAST(countryCode AS VARCHAR) AS country_code,
    {{ surrogate_key('continent', 'parentarea')}} AS continent_id,
    IF(CAST(parentArea AS VARCHAR) = 'N/C America', 'North America', CAST(parentArea AS VARCHAR)) AS continent_name,
    CAST(parentAreaId AS VARCHAR) AS parent_area_id
FROM areas
