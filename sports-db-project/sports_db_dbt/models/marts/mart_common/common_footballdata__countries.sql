WITH 

areas AS (
    SELECT * FROM {{ ref('stg_footballdata__areas') }}
)

SELECT 
    country_id,
    football_data_area_id,
    country_name,
    country_code,
    continent_id
FROM areas
WHERE country_name != 'World'
