from typing import Any, Iterable, Mapping, MutableMapping, Optional, Union, List
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

    def __init__(self, start_time: Union[str, datetime, None] = None, account_ids:
                 List[str] = [], **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.account_ids = account_ids

        # Set default start_time if not provided (5 days before current time)
        if not self.start_time:
            self.start_time = datetime.utcnow() - timedelta(days=5)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def should_retry(self, response: requests.Response) -> bool:
        """
        Determine if we should retry based on the response status code.
        Don't retry on 401/403 (auth errors) as they won't fix themselves.
        Do retry on 429 (rate limit) and 5xx (server errors).
        """
        if response.status_code in [401, 403]:
            logger.error(f"Authentication error ({response.status_code}). Not retrying. Response: {response.text[:200]}")
            return False
        if response.status_code == 429:
            logger.warning("Rate limit hit (429). Will retry with backoff.")
            return True
        if response.status_code >= 500:
            logger.warning(f"Server error ({response.status_code}). Will retry.")
            return True
        return super().should_retry(response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """Handle rate limiting and backoff"""
        # Only try to parse JSON if we haven't already
        try:
            response_data = response.text  # Use .text instead of .json() to avoid consuming the response
            logger.warning(f"API rate limit or error. Status: {response.status_code}, Headers: {response.headers}")
            logger.debug(f"Response body: {response_data}")
        except Exception as e:
            logger.warning(f"Could not log response: {e}")

        # Check for Retry-After header (Twitter uses this for rate limiting)
        delay_time = response.headers.get("Retry-After")
        if delay_time:
            logger.info(f"Rate limited. Retrying after {delay_time} seconds")
            return int(delay_time)

        if response.status_code == 429:
            logger.warning("Rate limit hit (429) but no Retry-After header. Using default 60 second backoff")
            return 60.0

        return None

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

class AccountsAdditional(TwitterStream):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, account_ids:
                 List[str]= [], **kwargs):
        super().__init__(start_time=start_time, account_ids=account_ids, **kwargs)

    @property
    def use_cache(self) -> bool:
        return True

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        limit_date = datetime.today() - timedelta(31)
        for account in self.account_ids:
            yield {"account_id": account}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"users/{stream_slice['account_id']}?user.fields=public_metrics,description,url,most_recent_tweet_id,pinned_tweet_id,created_at,verified_type,verified_followers_count,name,username,verified,parody"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data = response.json()['data']
        yield {
            "parody": data.get("parody"),
            "username": data.get("username"),
            "most_recent_tweet_id": data.get("most_recent_tweet_id"),
            "created_at": data.get("created_at"),
            "verified_type": data.get("verified_type"),
            "followers_count": data.get("public_metrics").get("followers_count"),
            "following_count": data.get("public_metrics").get("following_count"),
            "tweet_count": data.get("public_metrics").get("tweet_count"),
            "listed_count": data.get("public_metrics").get("listed_count"),
            "like_count": data.get("public_metrics").get("like_count"),
            "media_count": data.get("public_metrics").get("media_count"),
            "verified": data.get("verified"),
            "id": data.get("id"),
            "description": data.get("description"),
            "name": data.get("name")
        }

class Tweet(TwitterStream):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, account_ids:
                 List[str]= [], **kwargs):
        super().__init__(start_time=start_time, account_ids=account_ids, **kwargs)

    @property
    def use_cache(self) -> bool:
        return True

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        limit_date = datetime.today() - timedelta(31)
        for account in self.account_ids:
            yield {"account_id": account}


    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        logger.info("Getting tweet from account %s", stream_slice.get('account_id'))
        return f"users/{stream_slice['account_id']}/tweets"

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
        account_id = stream_slice.get("author_id")
        logger.debug("Fetching tweet %s from Account id %s", tweet_id, account_id)
        return f"tweets/{tweet_id}"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        limit_date = datetime.today() - timedelta(31)
        for parent_slice in super().stream_slices(sync_mode=SyncMode.full_refresh):
            tweet = parent_slice["parent"]
            if datetime.strptime(tweet.get("created_at"), "%Y-%m-%dT%H:%M:%S.%fZ") > limit_date:
                yield {"id": tweet.get('id'), "author_id": tweet.get("author_id")}
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
