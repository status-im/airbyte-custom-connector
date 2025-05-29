from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import requests
import time
from datetime import datetime
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")

class TagsStream(HttpStream):
    url_base = "https://api.x.com/2/"
    primary_key = "id"

    def __init__(self, start_time: str = None, account_id: str = None, tags: List[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.account_id = account_id
        self.tags = tags or []

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for tag in self.tags:
            yield {"tag": tag}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "tweets/search/recent" # this endpoint fetches data from the last 7 days 

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token' in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.debug('DBG-NT: %s', response.json()['meta']['next_token'])
            return {"next_token": response.json()['meta']['next_token']}

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        tag = stream_slice["tag"]
        params = {
            "query": tag,
            "tweet.fields": "text,public_metrics,author_id,referenced_tweets,created_at",
            "max_results": 100
        }
        params.update({"start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")})
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Full response %s", response.json())
        if 'data' in response.json():
            data = response.json()['data']
            for t in data:
                # Add the tag that matched this tweet
                t["matched_tag"] = stream_slice["tag"]
                yield t
        time.sleep(2)  # Rate limiting protection 