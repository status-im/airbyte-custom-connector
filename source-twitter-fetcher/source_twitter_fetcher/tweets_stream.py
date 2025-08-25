from typing import Any, Iterable, Mapping, MutableMapping, Optional, Union
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.models import SyncMode

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):
    url_base = "https://api.x.com/2/"

    def __init__(self, start_time: Union[str, datetime, None] = None, account_id: str = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.account_id = account_id
        
        # Set default start_time if not provided (5 days before current time)
        if not self.start_time:
            self.start_time = datetime.utcnow() - timedelta(days=5)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        logger.warn("API rate limit: %s\n%s", response.json(), response.headers)

        delay_time = response.headers.get("Retry-After")
        if delay_time:
            return int(delay_time)

    def _apply_rate_limiting(self):
        """Apply rate limiting delay that all Twitter streams should use"""
        time.sleep(2)

class Account(TwitterStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "users/me?user.fields=public_metrics,protected,description,url,most_recent_tweet_id,pinned_tweet_id,created_at,verified_type"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data = response.json()['data']
        yield data

class Tweet(TwitterStream):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, account_id: str = None, **kwargs):
        super().__init__(start_time=start_time, account_id=account_id, **kwargs)

    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"users/{self.account_id}/tweets"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token' in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.debug('DBG-NT: %s', response.json()['meta']['next_token'])
            return {"pagination_token": response.json()['meta']['next_token']}

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
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
                yield t
        self._apply_rate_limiting()

class TweetMetrics(HttpSubStream, Tweet):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, **kwargs):
        super().__init__(start_time=start_time, **kwargs)

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        tweet_id = stream_slice.get("id")
        logger.debug("Fetching tweet %s from Account id %s", tweet_id, self.account_id)
        return f"tweets/{tweet_id}"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        limit_date = datetime.today() - timedelta(31)
        for parent_slice in super().stream_slices(sync_mode=SyncMode.full_refresh):
            tweet = parent_slice["parent"]
            if datetime.strptime(tweet.get("created_at"), "%Y-%m-%dT%H:%M:%S.%fZ") > limit_date:
                yield {"id": tweet.get('id')}
            else:
                logger.info("Not calling full metrics endpoint for tweet %s, tweet too old", tweet.get('id'))

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "tweet.fields": "non_public_metrics,organic_metrics,created_at",
        }
        logger.debug(f"DBG-FULL - query params: %s", params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if 'data' in response.json():
            data = response.json()['data']
            logger.debug("DBG-FULL-T: id %s", data.get('id'))
            yield data
        self._apply_rate_limiting()

class TweetPromoted(HttpSubStream, Tweet):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, **kwargs):
        super().__init__(start_time=start_time, **kwargs)

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        tweet_id = stream_slice.get("id")
        logger.debug("Fetching tweet %s from Account id %s", tweet_id, self.account_id)
        return f"tweets/{tweet_id}"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        limit_date = datetime.today() - timedelta(31)
        for parent_slice in super().stream_slices(sync_mode=SyncMode.full_refresh):
            tweet = parent_slice["parent"]
            if datetime.strptime(tweet.get("created_at"), "%Y-%m-%dT%H:%M:%S.%fZ") > limit_date:
                yield {"id": tweet.get('id')}
            else:
                logger.info("Not calling promoted_metrics endpoint for tweet %s, tweet too old", tweet.get('id'))

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "tweet.fields": "promoted_metrics",
        }
        logger.debug(f"DBG-FULL - query params: %s", params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if 'data' in response.json():
            data = response.json()['data']
            yield data
        elif 'error' in response.json():
            logger.info("No promoted Metrics for this tweet")
        self._apply_rate_limiting()
