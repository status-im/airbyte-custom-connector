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
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):

  url_base = "https://api.twitter.com/2/"

  def __init__(self, api_key: str=None, accounts: List=None, start_time: str = None, stop_time: str = None, **kwargs):
    super().__init__(**kwargs)
    self.api_key = api_key
    self.accounts = accounts
    self.start_time = start_time
    self.stop_time = stop_time;

  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    return None

class TwitterAccountData(TwitterStream):

  primary_key = "account_id"

  @property
  def use_cache(self) -> bool:
    return True


  def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
    for account in self.accounts:
      yield {
        "name": account
      }


  def path(
    self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
  ) -> str:
    return f"users/by/username/{stream_slice['name']}?user.fields=public_metrics"

  def request_headers(
    self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return { 
      "Authorization" : f"Bearer {self.api_key}",
      "User-Agent": "v2RecentSearchPython"
    }

  def parse_response(
        self, 
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
  ) -> Iterable[Mapping]:
    logger.info("Getting data of %s account", stream_slice['name'])
    logger.info("Response: %s", response.json())
    data=response.json()['data']
    yield {
      "account_id": data['id'],
      "username": data['username'],
      "account_name": data['name'],
      "tweet_count": data['public_metrics']['tweet_count'],
      "like_count": data['public_metrics']['like_count'],
      "following_count": data['public_metrics']['following_count'],
      "follower_count": data['public_metrics']['followers_count'],
      "listed_count": data['public_metrics']['listed_count'],
    }
    time.sleep(2)


class TwitterTweet(HttpSubStream, TwitterAccountData):
    #TODO: See how to get the account ID
  primary_key = ""
  def __init__(self, **kwargs):
    super().__init__(TwitterAccountData(**kwargs),**kwargs)

  def path(
    self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
  ) -> str:
        account_id = stream_slice.get("parent").get("account_id")
        return f"users/{account_id}/tweets?tweet.fields=text,public_metrics,author_id,referenced_tweets,created_at&start_time={self.start_time}"

  def request_headers(
    self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return { 
      "Authorization" : f"Bearer {self.api_key}",
      "User-Agent": "v2RecentSearchPython"
    }

  def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    data=response.json()['data']
    logger.info("Response: %s", response.json())
    referenced_tweets=""
    for t in data:
      if "referenced_tweets" in t:
        for rt in t.get('referenced_tweets'):
          referenced_tweets += f"{rt.get('type')}:{rt.get('id')};"
      yield {
        "id": t['id'],
        "created_at": t.get('created_at'),
        "retweet_count": t.get('public_metrics').get('retweet_count'),
        "reply_count": t.get('public_metrics').get('reply_count'),
        "like_count": t.get('public_metrics').get('like_count'),
        "quote_count": t.get('public_metrics').get('quote_count'),
        "referenced_tweets": referenced_tweets
      }
    time.sleep(2)

# Source
class SourceTwitterFetcher(AbstractSource):
  def check_connection(self, logger, config) -> Tuple[bool, any]:
    return True, None

  def streams(self, config: Mapping[str, Any]) -> List[Stream]:       
    return [
      TwitterAccountData(
        api_key=config['api_key'],
        accounts=config['accounts']),
      TwitterTweet(
        api_key=config['api_key'],
        accounts=config['accounts'],
        start_time=config['start_time'], 
        stop_time=datetime.now().isoformat())
    ]
