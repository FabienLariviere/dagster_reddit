import os

from dagster import Definitions, load_assets_from_modules, config_from_yaml_strings
from dagster_postgres import DagsterPostgresStorage
from dagster_postgres.utils import get_conn_string

from . import assets
from .database import postgres_io_manager

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        'db_io_manager': postgres_io_manager.configured(
            get_conn_string(
                os.getenv('DAGSTER_PG_USERNAME'),
                os.getenv('DAGSTER_PG_PASSWORD'),
                os.getenv('DAGSTER_PG_HOST'),
                os.getenv('DAGSTER_PG_DB')
            ))
    }
)
