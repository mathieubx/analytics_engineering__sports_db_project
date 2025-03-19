#!/usr/bin/env python
# coding: utf-8

# In[4]:


# A pipeline coded the good old way

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

# /?dateFrom=2022-01-01
# /?dateTo=2022-01-10

def extract_load_football_data(
        endpoint = None,
        column_names = None,
        **params # Note: Arbitrary keyword arguments are dictionaries
):

    # Step 0: import API credentials
    secrets=toml.load("C:/Users/mbmbm/matbx-data/sports-db-project/sports_db_dlt/.dlt/secrets.toml")
    request_header=secrets['source']['football_data']['request_header']
    api_key=secrets['source']['football_data']['api_key']

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
    try:
        with duckdb.connect("C:/Users/mbmbm/matbx-data/sports-db-project/duckdb-databases/sports_db-raw.duckdb") as con:
            con.sql(f"CREATE OR REPLACE TABLE {endpoint} AS SELECT * FROM endpoint_df")
        message="Successfully pushed dataframe to DuckDB"
    except:
        message="Error, couldn't load data into DuckDB"

    return(message)


# In[5]:


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

# Teams
extract_load_football_data(
    endpoint='teams', 
    column_names=[ 
        "id",
        "area",
        "name",
        "shortName",
        "tla",
        "address",
        "website",
        "founded",
        "clubColors",
        "venue",
        "runningCompetitions",
        "coach",
        "marketValue",
        "squad",
        "staff",
        "lastUpdated"
    ]
)


# In[6]:


time.sleep(300) # We pause to make sure we don't exceed the number of request allowed per minute.

# Step 0: import API credentials
secrets=toml.load("C:/Users/mbmbm/matbx-data/sports-db-project/sports_db_dlt/.dlt/secrets.toml")
request_header=secrets['source']['football_data']['request_header']
api_key=secrets['source']['football_data']['api_key']

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
    print(url)
    print(requests.get(url, headers={request_header:api_key}).json())
    try:
        response = requests.get(url, headers={request_header:api_key}).json()['matches'] # This is a list
    except:
        print("Failed at api request")
    matches_list.append(response)

    if i%5==0: time.sleep(60) # Stop the program for 60 second each 5 requests. Why 5? Because it feels like the "10 requests per minute" are too much for the API


# In[24]:


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
with duckdb.connect("C:/Users/mbmbm/matbx-data/sports-db-project/duckdb-databases/sports_db-raw.duckdb") as con:
    con.sql("CREATE OR REPLACE TABLE matches AS SELECT * FROM matches_df")


# In[ ]:


# Quick check
with duckdb.connect("C:/Users/mbmbm/matbx-data/sports-db-project/duckdb-databases/sports_db-raw.duckdb") as con:
    con.sql("SELECT * FROM matches").show()
