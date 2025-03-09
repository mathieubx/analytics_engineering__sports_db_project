from typing import Any, Optional

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_resources,
    rest_api_source,
)


@dlt.source(name="free_test_api")
def free_test_api_source():
    # Create a REST API configuration for the GitHub API
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://freetestapi.com/api/v1",
            # we add an auth config if the auth token is present
            "auth": None
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            "books"
        ],
    }   

    yield from rest_api_resources(config)


def load_free_test_api():
    pipeline = dlt.pipeline(
        pipeline_name="free_test_api",
        # An absolute path is used here because I couldn't get dlt to load data in duckdb with a relative one. 
        destination=dlt.destinations.duckdb("C:/Users/mbmbm/matbx-data/sports-db-project/duckdb-databases/sports_db-raw.duckdb"),
        dataset_name="test_api_data",
    )

    load_info = pipeline.run(free_test_api_source())
    print(load_info) 

if __name__ == "__main__":
    load_free_test_api()