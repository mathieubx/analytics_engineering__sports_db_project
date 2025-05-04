{% test is_number_of_x_and_y_equal(model, column_name, x_value, y_value) %}

WITH 

number_of_values AS (
    SELECT
        COUNT(IF( {{column_name}} = '{{ x_value }}', 1, NULL )) AS number_of_x_values,
        COUNT(IF( {{column_name}} = '{{ y_value }}', 1, NULL )) AS number_of_y_values,
    FROM {{ model }}
)

SELECT
    *
FROM number_of_values 
WHERE number_of_x_values != number_of_y_values

{% endtest %}