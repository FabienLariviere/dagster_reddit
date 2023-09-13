import json
import re

import requests
from pydantic import BaseModel


class RawData(dict): ...


class RedditChild(BaseModel):
    name: str
    subreddit: str
    title: str
    selftext: str
    upvote_ratio: float
    ups: int
    created: float
    over_18: bool
    permalink: str
    stickied: bool


class Reddit:
    _base_url = 'https://www.reddit.com'
    _session = requests.Session()

    def __init__(self):
        self._session.headers['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 YaBrowser/23.7.5.704 Yowser/2.5 Safari/537.36'

    def compare_url(self, *parts: str):
        return '/'.join((self._base_url, *parts, '.json'))

    def get_page(self, board, category='new', limit=100) -> RawData:
        board = f'r/{board}'

        response = self._session.get(self.compare_url(board, category), params={'limit': limit})
        if response.status_code != 200:
            raise ConnectionError(response)

        return RawData(response.json())

    @staticmethod
    def get_children(data: RawData, skip_stickied=True) -> list[RedditChild]:
        children = data['data']['children']
        _children_output = []
        for child in children:
            reddit_child = RedditChild.parse_raw(json.dumps(child['data']))
            if skip_stickied and reddit_child.stickied:
                continue
            _children_output.append(reddit_child)
        return _children_output
