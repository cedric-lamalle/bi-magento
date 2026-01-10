from typing import Any

import dlt

from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import requests


custom_session = requests.Session()
custom_session.verify = False

# searchCriteria[currentPage]
@dlt.source(name="magento")
def magento_source() -> Any:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://magento.test/rest/V1/",
            "auth": {
                "type": "bearer",
                "token": dlt.secrets["magento_access_token"],
            },
            "session": custom_session,
        },
        "resource_defaults": {
            "primary_key": "entity_id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "searchCriteria[pageSize]": 100,
                },
                "data_selector": "items",
                "paginator": PageNumberPaginator(
                    page_param="searchCriteria[currentPage]", total_path="total_count"
                ),
            },
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_magento() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_magento",
        destination="duckdb",
        dataset_name="magento_data",
    )

    load_info = pipeline.run(magento_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_magento()
