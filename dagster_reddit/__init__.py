import os
from datetime import timedelta

from dagster import Definitions, load_assets_from_modules, define_asset_job, AssetSelection, ScheduleDefinition, EnvVar, \
    sensor, SkipReason
from dagster_postgres.utils import get_conn_string
from dagster_aws.s3 import S3Resource

from telebot import TeleBot

from . import assets
from .database import postgres_io_manager
from .hooks import telegram_message_on_failure, telegram_message_on_success
from .jobs import reddit_parse_job

all_assets = load_assets_from_modules([assets])

reddit_posts = define_asset_job("reddit_posts_job", selection=AssetSelection.groups('posts'),
                                hooks={telegram_message_on_failure, telegram_message_on_success})

posts_schedule = ScheduleDefinition('hourly_posts', job=reddit_posts, cron_schedule='0 * * * *')

bot = TeleBot(os.getenv('TGBOT_TOKEN'))
s3 = S3Resource(endpoint_url='http://s3.k9a8ops.online', bucket=os.getenv('S3_BUCKET'), region_name='us-east-1')


@sensor(job=reddit_posts, minimum_interval_seconds=300)
def logging_sensor(context):
    context.log.info("Logging from a sensor")
    return SkipReason("Nothing to do")


defs = Definitions(
    assets=all_assets,
    jobs=[reddit_posts, reddit_parse_job],
    schedules=[posts_schedule],
    sensors=[logging_sensor],
    resources={
        'tg': bot,
        's3': s3,
        'db_io_manager': postgres_io_manager.configured(
            get_conn_string(
                os.getenv('DAGSTER_PG_USERNAME'),
                os.getenv('DAGSTER_PG_PASSWORD'),
                os.getenv('DAGSTER_PG_HOST'),
                os.getenv('DAGSTER_PG_DB')
            ))
    }
)
