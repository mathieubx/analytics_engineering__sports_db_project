from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'mathieu',
    'email' : 'mbmbmbm10102000@gmail.com'
}

def coucou():
    print("coucou")

with DAG(
    'coucou_dag', 
    start_date = datetime(2025, 5, 24) ,
    schedule = '* * * * *',
    catchup = False
) as dag:

    extract_football_data = PythonOperator(
        task_id = 'coucou',
        python_callable = coucou
    )
