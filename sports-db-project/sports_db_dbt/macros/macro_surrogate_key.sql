{% macro surrogate_key(prefix, column_name) %}
    '{{ prefix }}' || '_' || md5( CAST( {{ column_name }} AS VARCHAR) )
{% endmacro %}