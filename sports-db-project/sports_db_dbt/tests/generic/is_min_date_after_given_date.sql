{% test is_min_date_after_given_date(model, column_name, interval) %}

SELECT
    *
FROM {{ model }}
-- Description of the condition : 
--      current_date - interval -> The date "interval" ago
--      FIRST_VALUE(column) OVER(ORDER BY column) -> The oldest date in column
--      Whole condition : The oldest date is actually after X Years/months/days ago.
--      Ex in stg_football_data__players -> The oldest player was born less than 50 years ago.
QUALIFY NOT FIRST_VALUE( {{ column_name }} ) OVER(ORDER BY {{ column_name }} ) > current_date - {{ interval }}

{% endtest %}