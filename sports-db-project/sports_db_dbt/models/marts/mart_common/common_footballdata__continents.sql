WITH 

areas AS (
    SELECT * FROM {{ ref("stg_footballdata__areas") }}
)

SELECT DISTINCT
    continent_id,
    continent_name
FROM areas
WHERE continent_id IS NOT NULL AND continent_name != 'World'
