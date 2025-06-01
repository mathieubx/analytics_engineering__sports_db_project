# Import packages

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import duckdb
import toml
import datetime
from datetime import datetime as dt 
import time
import json
from ast import literal_eval

# Define the Python callables:

###################################################################################################
########################### Definition of players extraction ######################################
###################################################################################################

def extract_football_players():
        
    # Set up: import API credentials
    secrets=toml.load('/opt/airflow/config/secrets.toml')
    request_header=secrets['source']['football_data']['request_header']
    api_key=secrets['source']['football_data']['api_key']

    # Set up: store path to DuckDB database 
    database_path = '/opt/airflow/duckdb/sports_db.duckdb'

    # The logic behind this script is the following: limited to 10 calls per minute with more than 250 000 players to extract from
    # the API, I would need 20 days to extract all players. Therefore, I will only extract the players I might actually need, which 
    # are the players from the 12 major leagues.
    # To do so, I will follow 3 steps : 
        # 1) Export the 'players' from the teams we already have in the database, which are the teams we are interested in
        # 2) Extract the players from the API based on their IDs
        # 3) Load them in the database

    # Step 1) Export the 'players' of the teams we have already extracted from the API
    with duckdb.connect(database_path) as con:
        all_players = con.sql(
            '''
            SELECT DISTINCT
            -- Getting the unique player_id of each player of each team we are interested in
                UNNEST(squad).id AS player_id,
            FROM raw.football_data__teams
            ''').df()
        all_players = all_players['player_id'].sort_values().to_list()

    # Splitting the lists into lists of 100 items, to make debugging easier if an error occurs during extraction
    # a) First, let's count the number of lists needed :
    number_of_lists = len(all_players)//100 + 1

    # b) Then, let's split the all_players list into {number_of_lists} lists
    i = 0
    all_players_lists = {}
    while i <= number_of_lists:
        start_index = i*100
        end_index = 100*(i + 1)
        all_players_lists[i] = all_players[start_index:end_index]
        i = i + 1

    # Step 2) Extract the players from the API

    # Initialization
    players_list = []
    start_time = time.time() # I want to take a look at how long the extraction is
    error_count = 0
    information_to_extract = [
        'id', 
        'name',
        'firstName',
        'lastName', 
        'dateOfBirth', 
        'nationality', 
        'section', 
        'position', 
        'shirtNumber', 
        'lastUpdated', 
    ]

    for index, players_ids_group in all_players_lists.items():
        i = 0
        
        for player_id in players_ids_group:
            i = i + 1
            try:
                url = f"https://api.football-data.org/v4/persons/{player_id}"
                response = requests.get(url, headers={request_header:api_key}).json()
                
                # Extract only wanted information thanks to dict comprehension. Also, we add two columns that were nested inside the json output
                dict_player = {key:value for (key,value) in response.items() if key in (information_to_extract)}
                dict_player['current_team_id'] = response['currentTeam']['id']
                dict_player['player_contract'] = response['currentTeam']['contract']
                # Adding the item to the players_list
                players_list.append(dict_player)
                if i%10 == 0: time.sleep(62)

            # If error, return and continue the loop:
            except:
                error_count = error_count + 1
                print("Group n°:", index)
                print("An error occured during this group's extraction")
                print(f"Error occurred around item {i} of the group")
                continue

    df_players = pd.DataFrame(players_list)
    with duckdb.connect(database_path) as con:
        con.sql('CREATE OR REPLACE TABLE raw.football_data__players AS SELECT * FROM df_players')

    end_time = time.time()
    extraction_time = end_time - start_time
    print("The program ran for", extraction_time, "seconds with", error_count, "errors.")

###################################################################################################
################################ Definition of the actual DAG #####################################
###################################################################################################

with DAG(
    'elt_football_dag_players',
    start_date = dt(2025,5,29),
    schedule = '0 0 1,11,21 * *',  # Extract players thrice a month
    catchup = False
) as dag:

    # Task 1: Extract players from API
    extract_players = PythonOperator(
        task_id = 'extract_players',
        python_callable = extract_football_players
    )

    # Task 2: Run dbt to update database
    run_dbt = BashOperator(
        task_id = 'run_dbt',
        bash_command = 'cd /opt/airflow/dbt && run dbt'
    )

extract_players >> run_dbt
