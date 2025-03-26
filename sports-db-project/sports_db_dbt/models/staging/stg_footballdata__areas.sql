WITH

areas AS (
    SELECT * FROM {{ source('sports_db_raw', 'areas') }}
)

SELECT
     * 
FROM areas
