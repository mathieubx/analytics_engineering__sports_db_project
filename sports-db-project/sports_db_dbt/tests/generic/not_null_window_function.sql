-- Custom test created in order to add a "qualify" argument to the not_null default test.
-- Emerged from the need to have: version_end_at not null if the version is the last one for the team/competition pair

{% test not_null_window_function(model, column_name, qualify=none) %}

SELECT
    *
FROM {{ model }}
WHERE {{ column_name }} IS NULL
QUALIFY {{ qualify }}

{% endtest %}