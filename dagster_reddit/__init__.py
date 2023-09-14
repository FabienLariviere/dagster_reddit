import os

from dagster import Definitions, load_assets_from_modules, config_from_yaml_strings, define_asset_job, AssetSelection, \
    ScheduleDefinition
from dagster_postgres import DagsterPostgresStorage
from dagster_postgres.utils import get_conn_string

from . import assets
from .database import postgres_io_manager

# from .jobs import reddit_parse_job

all_assets = load_assets_from_modules([assets])

reddit_posts = define_asset_job("reddit_posts_job", selection=AssetSelection.groups('posts'))
posts_schedule = ScheduleDefinition('hourly_posts', job=reddit_posts, cron_schedule='0 * * * *')

defs = Definitions(
    assets=all_assets,
    jobs=[reddit_posts],
    schedules=[posts_schedule],

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
