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

# Documentation --------------------------------------------------------------------------------------------------------
'''
This DAG aims to extract the following entities from the football-data API: matches, teams 
A few questions that might pop out:
    - Q: Why bothering to extract teams daily?
      A: Well, I want to be able to adapt to changes in squad in case I want to conduct an analysis at the player granularity.
         This requires a regular update of teams data.
'''

default_args = {
    'owner' : 'mathieu',
    'email' : 'mbmbmbm10102000@gmail.com'
}

###################################################################################################
########################### Definition of teams extraction ########################################
###################################################################################################


def extract_football_teams():
    # Extract teams
    print('Teams extraction running...')

    # Variables Intialization
    secrets=toml.load('/opt/airflow/config/secrets.toml')
    request_header=secrets['source']['football_data']['request_header']
    api_key=secrets['source']['football_data']['api_key']
    database_path = "/opt/airflow/duckdb/sports_db.duckdb"
    
    # The football-data.org API free plan grants us access to the 12 major football leagues.
    # Therefore, we can only access the teams data for these 12 leagues' teams
    # Strategy : iterate through each league ID and extract the teams that belong to them.

    time.sleep(100) # Letting the API rest (lol starting to crack up dev jokes)

    # Since we only have 12 leagues, I manually listed them below:
    major_league_ids = [
        2002,  # Bundesliga
        2013,  # Campeonato Brasileiro Série A
        2016,  # Championship
        2152,  # Copa Libertadores
        2003,  # Eredivisie
        2018,  # European Championship
        2015,  # Ligue 1
        2021,  # Premier League
        2017,  # Primeira Liga
        2014,  # Primera Division
        2019,  # Serie A
        2001   # UEFA Champions League
    ]

    i = 0 # Set a count to deal with the limit of 10 requests per minute

    # Step 1: Set up the loop

    # Note that there is a slight tweak here. European competitions' seasons overlap two calendar year. So, season 2024 of Ligue 1 is actually season 2024/2025
    # On the contrary, in South America, competitions start around the beginning of the year and end around the end of the year. So the 2024 season is really the 2024.
    # For this project's sake, we want to be able to analyze the current season.
    # This means season 2024 in Europe and season 2025 in South America. 
    # The extraction needs to take this point into account.

    for id in major_league_ids:
        # Extracting season 2025 of South American competitions
        if id in (2013, 2152):
            url = f"http://api.football-data.org/v4/competitions/{id}/teams?season=2025" 
        # Extracting season 2024 of European competitions
        else: 
            url = f"http://api.football-data.org/v4/competitions/{id}/teams?season=2024" # Extracting season 2024 of European competitions
        response = requests.get(url, headers={request_header:api_key}).json()
        i = i + 1
        
        #####################################################################################################################
        # (Notebook only)
        print()
        print(f"MAJOR LEAGUE ID : {id}")
        print(f"ITERATION N°{i}")
        print(response)
        print(response['teams'])
        #####################################################################################################################

        # Step 2: Create the DataFrame
        df = pd.DataFrame(response['teams'])

        # Step 2a: Add season and competition  
        # We add a few key information to the teams that are not contained in the 'teams' json's item: competition, season.
        # I don't necessarilly understand why, but to add json columns to a DataFrame, we need to pass it to the json.dumps() function.
        # For better understanding of the data structure, don't hesitate to print the "response" variable
        df['competition'] = json.dumps(response['competition']) 

        # Remove "winner" item with Dict Comprehension. Its null values make literal_eval() below crash
        season_without_winner = {k: v for k, v in response['season'].items() if k != "winner"}
        df['season'] = json.dumps(season_without_winner)

        # Step 2b: Handle string format of these columns
        # We will convert the "season" and "competition" columns values into dicts (the literal_eval() function does so). This will enable DuckDB to treat it as a STRUCT. Otherwise, we would end up with a VARCHAR column containing JSON. Doing this step in Python will make it easier to deal with the column in DBT further down the process.
        df['competition'] = df['competition'].apply(lambda x: literal_eval(x))
        df['season'] = df['season'].apply(lambda x: literal_eval(x)) 

        # Step 3: Concatenate the current DataFrame with the previous one
        # On the first iteration, the 'try' will fail, and create a first version of the 'full_df' (e.g. the df that stores all previous iterations)
        try:
            full_df = pd.concat([full_df, df])
        except:
            full_df = df

        # Set timer after 5 requests    
        if i%5 == 0: time.sleep(100)    

    teams_df = full_df
    teams_df.reset_index(inplace=True, drop=True)

    # And we finally have our teams source table, ready to be pushed to our raw DuckDB database
    with duckdb.connect(database_path) as con:
        con.sql("CREATE OR REPLACE TABLE raw.football_data__teams AS SELECT * FROM teams_df")

    print("Teams extracted.")

###################################################################################################
############################# Definition of the actual DAG ########################################
###################################################################################################

with DAG(
    'elt_football__teams', 
    start_date = dt(2025, 5, 24) ,
    schedule = '0 0 * * *',
    catchup = False
) as dag:

    # Task 1: Extract teams
    extract_teams = PythonOperator(
        task_id = 'extract_football_teams',
        python_callable = extract_football_teams
    )

    # Task 2: Run dbt
    run_dbt = BashOperator(
        task_id = 'run_dbt',
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

extract_teams >> run_dbt
