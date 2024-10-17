#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from airbyte_cdk.sources.streams.http.requests_native_auth import SingleUseRefreshTokenOauth2Authenticator

from .auth import TwitterOAuth

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):

    url_base = "https://api.x.com/2/"

    def __init__(self, start_time: str = None, account_id: str = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.account_id = account_id

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    # Lets see if the API is correctly made
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        logger.warn("API rate limit: %s\n%s", response.json(),response.hearders)

        delay_time = response.headers.get("Retry-After")
        if delay_time:
            return int(delay_time)

class Tweet(TwitterStream):
    primary_key = "id"

    @property
    def use_cache(self) -> bool:
        return True

    def path(
          self, stream_state: Mapping[str, Any] = None,
          stream_slice: Mapping[str, Any] = None,
          next_page_token: Mapping[str, Any] = None) -> str:
        return f"users/{self.account_id}/tweets"

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token' in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.debug('DBG-NT: %s', response.json()['meta']['next_token'])
            return {"pagination_token": response.json()['meta']['next_token']}

    def request_params(
        self, next_page_token: Optional[Mapping[str, Any]] = None,stream_state: Mapping[str, Any] = None,
          stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
                "tweet.fields" : "text,public_metrics,non_public_metrics,organic_metrics,author_id,referenced_tweets,created_at",
                "max_results": 100
            }
        # Add condition later:
        params.update({"start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")})
        if next_page_token:
            params.update(**next_page_token)
        logger.info(f"DBG - query params: %s", params)
        return params


    def parse_response(
            self, response: requests.Response,
            stream_slice: Mapping[str, Any] = None, **kwargs
        ) -> Iterable[Mapping]:
        logger.debug("Full response %s", response.json())
        if 'data' in response.json():
            data=response.json()['data']
            for t in data:
                logger.debug("DBG-T: id %s", t.get('id'))
                yield t
        time.sleep(2)

class TweetMetrics(HttpSubStream, Tweet):
    primary_key = "id"

    def path(
          self, stream_state: Mapping[str, Any] = None,
          stream_slice: Mapping[str, Any] = None,
          next_page_token: Mapping[str, Any] = None
    ) -> str:
        tweet_id = stream_slice.get("parent").get("id")
        logger.debug("Fetching tweet %s from Account id %s", tweet_id, self.account_id)
        return f"tweets/{tweet_id}"

    def request_params(
            self, stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {
                "tweet.fields" : "non_public_metrics,organic_metrics,created_at",
            }
        # Add condition later:
        logger.debug(f"DBG-FULL - query params: %s", params)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.info("Twtter Response: %s", response.json())
        if 'data' in response.json():
            data=response.json()['data']
            logger.debug("DBG-FULL-T: id %s", data.get('id'))
            yield data
        time.sleep(2)

# Source
class SourceTwitterFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth=TwitterOAuth(
                config, token_refresh_endpoint="https://api.x.com/2/oauth2/token")
        tweet = Tweet(
                authenticator=auth,
                account_id=config["account_id"],
                start_time=datetime.strptime(config['start_time'], "%Y-%m-%dT%H:%M:%SZ"),
            )
        tweetMetrics = TweetMetrics(
                authenticator=auth,
                account_id=config['account_id'],
                parent=tweet
            )
        return [
            tweet,
            tweetMetrics
        ]
