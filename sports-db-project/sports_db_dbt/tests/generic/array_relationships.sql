{% test array_relationships(model, column_name, to, field) %}
 
WITH

unnested_array AS (
    SELECT
        UNNEST( {{ column_name }} ) AS {{ field }}
    FROM {{ model }}
),

to_table AS (
    SELECT * FROM {{ to }}
)

SELECT
    unnested_array.{{ field }}
FROM unnested_array
LEFT JOIN to_table USING ( {{ field }} )
WHERE to_table.{{ field }} IS NULL

{% endtest %}