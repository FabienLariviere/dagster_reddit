from dagster import asset, AssetExecutionContext, MetadataValue

from .models import Reddit, RedditChild
import pandas as pd


@asset(io_manager_key='db_io_manager')
def reddit_data(context: AssetExecutionContext) -> pd.DataFrame:
    reddit = Reddit()
    data = reddit.get_page('Python')
    children = Reddit.get_children(data)
    df = pd.DataFrame(child.dict() for child in children)

    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(df.head().to_markdown())
        }
    )
    return df


@asset
def get_best_new_posts(context: AssetExecutionContext, reddit_data: pd.DataFrame):
    sorted_data = reddit_data.sort_values(by=['upvote_ratio'], ascending=False)
    context.add_output_metadata(
        metadata={
            "preview": MetadataValue.md(sorted_data.head().to_markdown())
        }
    )
