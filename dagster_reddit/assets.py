from dagster import asset, AssetExecutionContext, MetadataValue, In, AssetIn, DagsterType, EnvVar
from dagster_aws.s3 import S3Resource

from .models import Reddit
import pandas as pd


@asset(io_manager_key='db_io_manager', group_name='posts')
def reddit_data(context: AssetExecutionContext) -> pd.DataFrame:
    reddit = Reddit()
    data = reddit.get_page('Python', 'hot')
    children = Reddit.get_children(data)
    df = pd.DataFrame(child.dict() for child in children)

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
    return df


@asset(group_name='posts', ins={
    'posts': AssetIn(key='reddit_data')
})
def get_best_posts(context: AssetExecutionContext, posts: pd.DataFrame):
    sorted_data = posts.sort_values(by=['ups', 'upvote_ratio'], ascending=False)
    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(sorted_data.head().to_markdown())
        }
    )


@asset(group_name='posts', ins={
    'posts': AssetIn(key='reddit_data')
})
def get_actual_posts(context: AssetExecutionContext, posts: pd.DataFrame, s3: S3Resource):
    client = s3.get_client()

    sorted_data = posts.sort_values(by=['created', 'ups'], ascending=False)
    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(sorted_data.head().to_markdown())
        }
    )
