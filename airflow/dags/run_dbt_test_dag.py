from airflow import DAG
from datetime import datetime as dt
import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import duckdb
import toml
import time
import json
from ast import literal_eval

# Actual DAG -----------------------------------------------------------------------------------------------------------
with DAG(
    'run_dbt_test', 
    start_date = dt(2025, 5, 24) ,
    schedule = '0 0 * * *',
    catchup = False
) as dag:

    run_dbt_task = BashOperator(
        task_id = 'run_dbt',
        bash_command = 'dbt run'
    )

run_dbt_task