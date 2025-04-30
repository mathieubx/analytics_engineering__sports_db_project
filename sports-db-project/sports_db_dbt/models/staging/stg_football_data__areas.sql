WITH

areas AS (
    SELECT * FROM {{ source('sports_db_raw', 'football_data__areas') }}
)

SELECT
    {{ surrogate_key('country', 'name') }} AS country_id,
    CAST(id AS VARCHAR) AS area_id,
    CAST(name AS VARCHAR) AS country_name,
    CAST(countryCode AS VARCHAR) AS country_code,
    {{ surrogate_key('continent', 'parentarea')}} AS continent_id,
    IF(CAST(parentArea AS VARCHAR) = 'N/C America', 'North America', CAST(parentArea AS VARCHAR)) AS continent_name,
    CAST(parentAreaId AS VARCHAR) AS parent_area_id
FROM areas
