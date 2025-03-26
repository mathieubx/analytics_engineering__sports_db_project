# A pipeline coded the good old way
print("running...")
# Let's build a function for the football data source. 
# Its arguments will be: 
#   endpoint, 
#   params (I'll start with data from the 2023/2024 season),   
#   column names (list)
# That way we can call the function without copy pasting the code again and again for each endpoint

import requests
import pandas as pd
import duckdb
import toml
import datetime
from datetime import datetime as dt 
import time
import json
from ast import literal_eval

# /?dateFrom=2022-01-01
# /?dateTo=2022-01-10

# Initialization: Let's create the needed variables
secrets=toml.load("C:/Users/mbmbm/matbx-data/sports-db-project/sports_db_pipeline/.dlt/secrets.toml")
request_header=secrets['source']['football_data']['request_header']
api_key=secrets['source']['football_data']['api_key']
database_path = "C:/Users/mbmbm/matbx-data/sports-db-project/duckdb-databases/sports_db.duckdb"

def extract_load_football_data(
        endpoint = None,
        column_names = None,
        **params # Note: Arbitrary keyword arguments are dictionaries
):

    # Step 1: Define request URL
    # If there are no parameters passed to the function (meaning: params is an empty dictionary), take the whole endpoint
    if params == {}: 
        url = f'https://api.football-data.org/v4/{endpoint}'
    else:
        url = f'https://api.football-data.org/v4/{endpoint}/?dateFrom={params['start_date']}&dateTo={params['end_date']}' # Note: we access each params' key's value as in any dictionary

    # Step 2: Make the GET request to the API, using a header to authenticate through API key
    response = requests.get(url, headers={request_header:api_key})

    # Step 3: Convert the JSON returned to a pandas DataFrame
    endpoint_df = pd.DataFrame(
        data=response.json()[f'{endpoint}'],
        columns = column_names
    )

    # DuckDB documentation to load data from pandas: https://duckdb.org/docs/stable/guides/python/import_pandas.html
    # Here, the "with" structure is a context manager that enables to close the connection (created on the just above line) automatically
    # Note that we prefix the table name with "raw", which specifies the schema we want to populate
    try:
        with duckdb.connect(database_path) as con:
            con.sql(f"CREATE OR REPLACE TABLE raw.football_data__{endpoint} AS SELECT * FROM endpoint_df")
        message="Successfully pushed dataframe to DuckDB"
    except:
        message="Error, couldn't load data into DuckDB"

    return(message)

# Calling pipelines

# Competitions
extract_load_football_data(
    endpoint='competitions', 
    column_names=[
        'id', 
        'area', 
        'name', 
        'code', 
        'type', 
        'currentSeason', 
        'lastUpdated'
    ]
)

# Areas
extract_load_football_data(
    endpoint='areas', 
    column_names=[ 
        "id",
        "name",
        "countryCode",
        "flag",
        "parentAreaId",
        "parentArea"
    ]
)

print('Areas and competitions extracted.')

# Extract matches

time.sleep(100)

# Seems like data is available from 2024-06-01
# Also, specified period cannot exceed 10 days
# What to do in this section: Build 10-days periods and extract matches for each and every one of them

# Step 1: Build 10-days periods 

# Intialization
start_date = dt.strptime("2024-08-01", "%Y-%m-%d") # From around the start of the major leagues' 2024/25 season
end_date = dt.now()
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
    period_end_date = dt.strftime(period['end_date'] - datetime.timedelta(days = 1), "%Y-%m-%d") # Substracting 1 day to avoid duplicating last day of the period

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

# And we finally have our matches source table, ready to be pushed to our raw DuckDB database
with duckdb.connect(database_path) as con:
    con.sql("CREATE OR REPLACE TABLE raw.football_data__matches AS SELECT * FROM matches_df")

print("Matches extracted.")

# Extract teams

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
#####################################################################################################################
# (Notebook only)
full_df = pd.DataFrame() # Creating an empty DataFrame to prevent from working with an already existing one 
#####################################################################################################################

# Step 1: Set up the loop
for id in major_league_ids:
    url = f"http://api.football-data.org/v4/competitions/{id}/teams?season=2024"
    response = requests.get(url, headers={request_header:api_key}).json()

    i = i + 1
    
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

    # Set timer after 10 requests    
    if i%10 == 0: time.sleep(70)    

teams_df = full_df
teams_df.reset_index(inplace=True, drop=True)

# And we finally have our teams source table, ready to be pushed to our raw DuckDB database
with duckdb.connect(database_path) as con:
    con.sql("CREATE OR REPLACE TABLE raw.football_data__teams AS SELECT * FROM teams_df")

print("Teams extracted.")

print("end of running.")
