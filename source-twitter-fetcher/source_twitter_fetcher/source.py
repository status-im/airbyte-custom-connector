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

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):

    url_base = "https://api.twitter.com/2/"

    def __init__(self, start_time: str = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    # Lets see if the API is correctly made
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        logger.warn("API rate limit: %s\n%s", response.json(),response.hearders)

        delay_time = response.headers.get("Retry-After")
        if delay_time:
            return int(delay_time)

class Account(TwitterStream):

    primary_key = "id"

    @property
    def use_cache(self) -> bool:
        return True

    def path(
          self, stream_state: Mapping[str, Any] = None, 
          stream_slice: Mapping[str, Any] = None, 
          next_page_token: Mapping[str, Any] = None
          ) -> str:
        return f"users/me?user.fields=public_metrics"

    def parse_response(
          self, 
          response: requests.Response,
          stream_slice: Mapping[str, Any] = None,
          **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Response: %s", response.json())
        data=response.json()['data']
        yield data
        # Wait to avoid reaching API limit
        time.sleep(2)

class Tweet(HttpSubStream, Account):
    primary_key = "id"

    def path(
          self, stream_state: Mapping[str, Any] = None, 
          stream_slice: Mapping[str, Any] = None, 
          next_page_token: Mapping[str, Any] = None
    ) -> str:
        account_id = stream_slice.get("parent").get("id")
        logger.info("Account id %s", account_id)
        #return f"users/{account_id}/tweets?tweet.fields=text,public_metrics,non_public_metrics,organic_metrics,author_id,referenced_tweets,promoted_metrics,created_at"
        return f"users/{account_id}/tweets"

    def next_page_token(self,  response: requests.Response) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token'  in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.info('DBG-NT: %s', response.json()['meta']['next_token'])
            return {"pagination_token": response.json()['meta']['next_token']}
    
    def request_params(
            self, stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
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

    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.info("Twtter Response: %s", response.json())
        if 'data' in response.json():
            data=response.json()['data']
            for t in data:
                logger.info("DBG-T: id %s", t.get('id'))
                yield t
        time.sleep(2)

class TweetFullHistory(HttpSubStream, Account):
    primary_key = "id"

    def path(
          self, stream_state: Mapping[str, Any] = None, 
          stream_slice: Mapping[str, Any] = None, 
          next_page_token: Mapping[str, Any] = None
    ) -> str:
        account_id = stream_slice.get("parent").get("id")
        logger.info("Account id %s", account_id)
        #return f"users/{account_id}/tweets?tweet.fields=text,public_metrics,non_public_metrics,organic_metrics,author_id,referenced_tweets,promoted_metrics,created_at"
        return f"users/{account_id}/tweets"

    def next_page_token(self,  response: requests.Response) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token'  in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.info('DBG-FULL-NT: %s', response.json()['meta']['next_token'])
            return {"pagination_token": response.json()['meta']['next_token']}
    
    def request_params(
            self, stream_state: Optional[Mapping[str, Any]],
            stream_slice: Optional[Mapping[str, Any]] = None,
            next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {
                "tweet.fields" : "text,public_metrics,author_id,referenced_tweets,created_at",
                "max_results": 100
            }
        # Add condition later:
        params.update({"start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")})
        if next_page_token:
            params.update(**next_page_token)
        logger.info(f"DBG-FULL - query params: %s", params)
        return params

    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.info("Twtter Response: %s", response.json())
        if 'data' in response.json():
            data=response.json()['data']
            for t in data:
                logger.info("DBG-FULL-T: id %s", t.get('id'))
                yield t
        time.sleep(2)

# Source
class SourceTwitterFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth=SingleUseRefreshTokenOauth2Authenticator(
                config, token_refresh_endpoint="https://api.twitter.com/2/oauth2/token")
        account =Account(authenticator=auth, start_time=datetime.strptime(config['start_time'], "%Y-%m-%d"))
        tweet = Tweet(
                authenticator=auth, 
                start_time=datetime.strptime(config['start_time'], "%Y-%m-%d"),
                parent=account
            )
        historycallTweet = TweetFullHistory(
                authenticator=auth, 
                start_time=datetime.strptime(config['start_time'], "%Y-%m-%d"),
                parent=account
            )
        return [
            account,
            tweet,
            historycallTweet
        ]
