WITH 

areas AS (
    SELECT * FROM {{ ref('stg_football_data__areas') }}
)

SELECT 
    country_id,
    area_id,
    country_name,
    country_code,
    continent_id
FROM areas
WHERE country_name != 'World'
