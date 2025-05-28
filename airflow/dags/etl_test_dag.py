from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime as dt
import duckdb

def connect_to_db():
    database_path = "/opt/airflow/duckdb/sports_db.duckdb"
    with duckdb.connect(database_path) as con:
        con.query('SELECT * FROM raw.football_data__matches LIMIT 5').show()

with DAG(
    'etl_test',
    start_date = dt(2025, 5, 27),
    catchup = False
) as dag:
    
    test_connexion = PythonOperator(
        task_id = 'connect_to_db',
        python_callable = connect_to_db
    )

    dbt_run = BashOperator(
        task_id = 'dbt_run',
        bash_command = 'dbt run'
    )


test_connexion >> dbt_run
