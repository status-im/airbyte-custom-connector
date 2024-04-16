#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
import time
from datetime import datetime 
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth.core import HttpAuthenticator
from airbyte_cdk.sources.streams.http.requests_native_auth import SingleUseRefreshTokenOauth2Authenticator

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):

    url_base = "https://api.twitter.com/2/"

    def __init__(self, start_time: str = None, stop_time: str = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.stop_time = stop_time;

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


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

    def __init__(self, **kwargs):
        super().__init__(Account(**kwargs),**kwargs)

    def path(
          self, stream_state: Mapping[str, Any] = None, 
          stream_slice: Mapping[str, Any] = None, 
          next_page_token: Mapping[str, Any] = None
    ) -> str:
        account_id = stream_slice.get("parent").get("id")
        logger.info("Account id %s", account_id)
        return f"users/{account_id}/tweets?tweet.fields=text,public_metrics,non_public_metrics,organic_metrics,author_id,referenced_tweets,promoted_metrics,created_at"
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.debug("Twtter Response: %s", response.json())
        data=response.json()['data']
        for t in data:
            yield t
        time.sleep(2)

# Source
class SourceTwitterFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth=SingleUseRefreshTokenOauth2Authenticator(
                config, token_refresh_endpoint="https://api.twitter.com/2/oauth2/token") 
        return [
            Account(authenticator=auth),
            Tweet(authenticator=auth, start_time=config['start_time'],
                stop_time=datetime.now().isoformat()
            )
        ]
