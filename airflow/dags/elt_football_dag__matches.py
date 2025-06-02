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

default_args = {
    'owner' : 'mathieu',
    'email' : 'mbmbmbm10102000@gmail.com'
}

###################################################################################################
########################### Definition of matches extraction ######################################
###################################################################################################


def extract_football_matches():
    print("Matches extraction running...")
    
    # Step 1: Build 10-days periods 
    # Variables Intialization
    secrets=toml.load('/opt/airflow/config/secrets.toml')
    request_header=secrets['source']['football_data']['request_header']
    api_key=secrets['source']['football_data']['api_key']
    database_path = "/opt/airflow/duckdb/sports_db.duckdb"

    # Initialization
    start_date = dt.strptime("2024-08-01", "%Y-%m-%d") # From around the start of the major leagues' 2024/25 season
    end_date = dt.now() + pd.DateOffset(months=12) # Add 12 months to ensure we capture all matches in a season. 
    date_iterator = start_date
    periods = []

    while date_iterator <= end_date:
        start_date = date_iterator
        date_iterator = date_iterator + datetime.timedelta(days=10)
        periods.append({"start_date":start_date, "end_date":date_iterator})

    ten_days_periods = pd.DataFrame(periods)
    
    # Step 2: Pull data from API for each 10-days period to get all matches for the current season
    # It is clearly specified in the documentation that the max number of requests per minute is 10. We set a 60 second timer after each 10 requests.

    i = 0 # Setting a counter (i) to rest 60 seconds after 10 requests
    matches_list = [] # Setting an empty list to store API calls results

    for index, period in ten_days_periods.iterrows():
        i = i + 1
        period_start_date = dt.strftime(period['start_date'], "%Y-%m-%d") # Format for inserting the date in the request's url
        period_end_date = dt.strftime(period['end_date'], "%Y-%m-%d") # Note that the end_date is also the first_date of the next period. This should lead to duplication in the API requests, but will not because the "dateTo" filter of the API is exclusive
        
        # Making the actual requests
        url = f"https://api.football-data.org/v4/matches/?dateFrom={period_start_date}&dateTo={period_end_date}"
        try:
            response = requests.get(url, headers={request_header:api_key}).json()['matches'] # This is a list
        except:
            print("Failed at api request")
        matches_list.append(response)

        if i%5==0: time.sleep(60) # Stop the program for 60 second each 5 requests. Why 5? Because it feels like the "10 requests per minute" are too much for the API

    # Now, the only thing we still need to do is concatenate the requests outputs together (equivalent of UNION ALL in SQL)

    # First, let's get all the columns we will have in the final matches DataFrame
    matches_columns = list(pd.DataFrame(matches_list[0]).columns)

    # Creating an empty DataFrame
    matches_df = pd.DataFrame(columns = matches_columns)

    # Now, each element in matches_list is a list of dictionaries. We convert each element to a DataFrame, and concatenate it to the matches_df

    for item in matches_list:
        matches_df = pd.concat([matches_df, pd.DataFrame(item)])

    matches_df.reset_index(inplace=True, drop=True)

    # In the source, the 'score' column is made of nested json items. This translate in python to a dictionaries of dictionaries.
    # Unfortunately DuckDB doesn't handle dicts of dicts and loads them as a VARCHAR, making hard to manipulate them in SQL
    # To deal with this situation, we break the 'score' column into other columns
    matches_df['winner'] = matches_df['score'].map(lambda x: x['winner']) # Accessing each 'winner' item of each row. Did so because matches_df['score']['winner'] doesn't work cause df['score'] returns a series of dicts. The series per say doesn't have a 'winner' key.
    matches_df['duration'] = matches_df['score'].map(lambda x: x['duration'])
    matches_df['full_time'] = matches_df['score'].map(lambda x: x['fullTime'])
    matches_df['half_time'] = matches_df['score'].map(lambda x: x['halfTime'])
    matches_df = matches_df.drop(columns='score')

    # And we finally have our matches source table, ready to be pushed to our raw DuckDB database
    with duckdb.connect(database_path) as con:
        con.sql("CREATE OR REPLACE TABLE raw.football_data__matches AS SELECT DISTINCT * FROM matches_df")

    print("Matches extracted.")

###################################################################################################
############################# Definition of the actual DAG ########################################
###################################################################################################

with DAG(
    'elt_football__matches', 
    start_date = dt(2025, 5, 24) ,
    schedule = '0 2 * * *',
    catchup = False
) as dag:

    # Task 1: Extract matches
    extract_matches = PythonOperator(
        task_id = 'extract_football_matches',
        python_callable = extract_football_matches
    )

    # Task 2: Run dbt
    run_dbt = BashOperator(
        task_id = 'run_dbt',
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

extract_matches >> run_dbt
