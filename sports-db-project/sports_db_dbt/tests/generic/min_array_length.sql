{% test min_array_length(model, column_name, array_length, or_equal=false) %}

SELECT 
    * 
FROM {{ model }}
{% if or_equal %}
WHERE NOT ARRAY_LENGTH( {{ column_name }} ) >= {{ array_length }} 
{% else %}
WHERE NOT ARRAY_LENGTH( {{ column_name }} ) > {{ array_length }}
{% endif %}

{% endtest %}