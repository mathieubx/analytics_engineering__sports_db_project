-- Custom test made to integrate @jpmmcneill 's tweaks to the dbt_utils.expression_is_true test
-- Here's the PR he made to add the possibility to include window functions conditions to the test https://github.com/dbt-labs/dbt-utils/pull/507
-- Unfortunately, the PR was never merge into the main branch

{% test window_expression_is_true(model, expression, column_name, condition) %}
 
 with meet_condition as (
     select * from {{ model }} where {{ condition }}
     select
       *,
       {% if column_name is none %}
       {{ expression }}
       {%- else %}
       {{ column_name }} {{ expression }}
       {%- endif %}
       as _dbt_utils_test_expression_passed
     from {{ model }}
     where {{ condition }}
 )
 
 select
     *
 from meet_condition
 {% if column_name is none %}
 where not({{ expression }})
 {%- else %}
 where not({{ column_name }} {{ expression }})
 {%- endif %}
 select * from meet_condition
 where not(_dbt_utils_test_expression_passed)
 
 {% endtest %}