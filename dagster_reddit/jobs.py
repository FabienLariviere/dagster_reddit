from dagster import Config, In, op, Out, OpExecutionContext, RunConfig, job
import pandas as pd

from dagster_reddit.models import Reddit


class RedditParseConfig(Config):
    board: str
    category: str
    count: int
    skip_pinned: bool


@op(ins={
    'board': In(default_value='Python', dagster_type=str),
    'category': In(default_value='hot', dagster_type=str),
    'count': In(default_value=100, dagster_type=int),
    'skip_pinned': In(default_value=True, dagster_type=bool),
    },
    out={
        'reddit_data': Out(dagster_type=pd.DataFrame, is_required=True)
    }
)
def reddit_parse_op(context: OpExecutionContext, board, category, count, skip_pinned) -> pd.DataFrame:
    reddit = Reddit()
    data = reddit.get_page(board, category, count)
    children = Reddit.get_children(data, skip_pinned)
    df = pd.DataFrame(child.dict() for child in children)

    return df


@job(config=RunConfig(
        ops={
            "reddit_parse_op": RedditParseConfig(board='Python', category='hot', count=100, skip_pinned=True)
        }
    )
)
def reddit_parse_job():
    reddit_parse_op()
