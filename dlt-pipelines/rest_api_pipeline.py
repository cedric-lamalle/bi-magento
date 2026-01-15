from typing import Any

import dlt

from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
import requests
import logging
from datetime import datetime
import urllib3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('magento_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
                "token": "xqhcm6bgr0ox036pk93ocmnlz7izo9ma",
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
                    base_page=1,
                    page_param="searchCriteria[currentPage]",
                    total_path="total_count",
                ),
            },
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "orders",
                    "params": {
                        "searchCriteria[sortOrders][0][field]": "entity_id",
                        "searchCriteria[sortOrders][0][direction]": "ASC",
                        # Carga incremental baseado em updated_at
                        "searchCriteria[filter_groups][0][filters][0][field]": "updated_at",
                        "searchCriteria[filter_groups][0][filters][0][condition_type]": "gteq",
                        "searchCriteria[filter_groups][0][filters][0][value]":  {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2020-01-01 00:00:00",
                        },
                    },    
                },
            },

            {
                "name": "products",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "products",
                    "params": {
                        "searchCriteria[pageSize]": 100,
                        "searchCriteria[sortOrders][0][field]": "id",
                        "searchCriteria[sortOrders][0][direction]": "ASC",
                        # Carga incremental baseado em updated_at
                        "searchCriteria[filter_groups][0][filters][0][field]": "updated_at",
                        "searchCriteria[filter_groups][0][filters][0][condition_type]": "gteq",
                        "searchCriteria[filter_groups][0][filters][0][value]": {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2020-01-01 00:00:00",
                        },
                    },
                    "data_selector": "items",
                    "paginator": PageNumberPaginator(
                        base_page=1,
                        page_param="searchCriteria[currentPage]",
                        total_path="total_count",
                    ),
                },
            },

            {
                "name": "customers",
                "primary_key": "id",
                "endpoint": {
                    "path": "customers/search",
                    "params": {
                        "searchCriteria[sortOrders][0][field]": "id",
                        "searchCriteria[sortOrders][0][direction]": "ASC",
                        "searchCriteria[filter_groups][0][filters][0][field]": "updated_at",
                        "searchCriteria[filter_groups][0][filters][0][condition_type]": "gteq",
                        "searchCriteria[filter_groups][0][filters][0][value]":  {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2020-01-01 00:00:00",
                        },
                    },    
                },
            },
            {
                "name": "categories",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "categories/list",
                    "params": {
                        "searchCriteria[sortOrders][0][field]": "id",
                        "searchCriteria[sortOrders][0][direction]": "ASC",
                        "searchCriteria[filter_groups][0][filters][0][field]": "updated_at",
                        "searchCriteria[filter_groups][0][filters][0][condition_type]": "gteq",
                        "searchCriteria[filter_groups][0][filters][0][value]":  {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2020-01-01 00:00:00",
                        },
                    }    
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
