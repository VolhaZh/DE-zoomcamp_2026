"""NYC taxi REST API pipeline using dlt."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_source():
    """Build a REST API source for paginated NYC taxi trip data."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    "path": "",
                    "params": {
                        "page": 1,
                    },
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    progress="log",
)


if __name__ == "__main__":
    info = pipeline.run(nyc_taxi_source())
    print(info)  # noqa: T201
