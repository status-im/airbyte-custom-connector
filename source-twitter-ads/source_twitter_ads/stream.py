from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import requests
from requests_oauthlib import OAuth1
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream

logger = logging.getLogger("airbyte")
DATE_FORMAT="%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_2="%Y-%m-%d"
class TwitterAdsStream(HttpStream):
  url_base = "https://ads-api.x.com/12/"
  accounts = []
  def __init__(self, start_time: str = None, account_ids: List[str] = [], **kwargs):
    super().__init__(**kwargs)
    self.start_time = start_time
    self.account_ids = account_ids
    logger.info(account_ids)

  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    return None

  def backoff_time(self, response: requests.Response) -> Optional[float]:
    logger.warn("API rate limit: %s\n%s", response.json(), response.headers)

    delay_time = response.headers.get("Retry-After")
    if delay_time:
      return int(delay_time)

class Account(TwitterAdsStream):
  primary_key = "id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    return f"accounts"

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' in response.json():
      for account in response.json()['data']:
        yield account
    else:
      logger.warn("No data in the account response")

class AdvertisementCampaign(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    account_id=stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/line_items"

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' in response.json():
      for line_item in response.json()['data']:
        yield line_item
    else:
      account_name=stream_slice.get("parent").get("name")
      logger.warn("No data in the line_item response for %s account", account_name)

class PromotedTweetActive(TwitterAdsStream):
   #fetch the active promoted twwet ids
  primary_key = "entity_id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    return f"stats/accounts/{stream_slice['account_id']}/active_entities"

  def stream_slices(
    self,
    sync_mode=None,
    stream_state: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Optional[Mapping[str, Any]]]:
    logger.info(self.account_ids)
    for account in self.account_ids:
      logger.info("account %s", account)
      yield {"account_id": account}

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return {
      "entity": "PROMOTED_TWEET",
      "start_time": self.start_time.strftime(DATE_FORMAT),
      "end_time": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime(DATE_FORMAT)
    }

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' in response.json():
      for entity in response.json()['data']:
        entity['account_id']=stream_slice['account_id']
        yield entity
    time.sleep(2)


class PromotedTweetBilling(HttpSubStream, PromotedTweetActive):
  #gets billing info for each promotted tweet
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    return f"stats/accounts/{stream_slice['account_id']}"

  def stream_slices(
    self,
    sync_mode=None,
    stream_state: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Optional[Mapping[str, Any]]]:
    for parent_slice in super().stream_slices(sync_mode=sync_mode):
      active_tweet = parent_slice["parent"]
      if "ALL_ON_TWITTER" in active_tweet.get("placements", []):
        yield {
          "account_id": active_tweet['account_id'],
          "promoted_tweet_id": active_tweet.get("entity_id"),
          "activity_start_time": active_tweet.get("activity_start_time"),
          "activity_end_time": active_tweet.get("activity_end_time")
        }

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    promoted_tweet_id = stream_slice.get("promoted_tweet_id") if stream_slice else None
    return {
      "entity": "PROMOTED_TWEET",
      "entity_ids": promoted_tweet_id,
      "granularity": "DAY",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "BILLING",
      "start_time": (datetime.now() - timedelta(days=7)).strftime(DATE_FORMAT_2),
      "end_time": datetime.now().strftime(DATE_FORMAT_2),

    }

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' in response.json():
      data = response.json()['data']
      for record in data:
        billing_data = {
          "id": stream_slice.get("promoted_tweet_id"),
          "activity_start_time": stream_slice.get("activity_start_time"),
          "activity_end_time": stream_slice.get("activity_end_time"),
          "billed_engagements": record.get("billed_engagements", []),
          "billed_charge_local_micro": record.get("billed_charge_local_micro", []),
          "account_id": stream_slice['account_id'],
          **record
        }
        yield billing_data
      time.sleep(1)

class PromotedTweetEngagement(HttpSubStream, PromotedTweetActive):
  # fetches engagement metrics on promoted tweets
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    return f"stats/accounts/{stream_slice['account_id']}"

  def stream_slices(
    self,
    sync_mode=None,
    stream_state: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Optional[Mapping[str, Any]]]:
    for parent_slice in super().stream_slices(sync_mode=sync_mode):
      active_tweet = parent_slice["parent"]
      if "ALL_ON_TWITTER" in active_tweet.get("placements", []):
        yield {
          "promoted_tweet_id":  active_tweet.get("entity_id"),
          "activity_start_time":  active_tweet.get("activity_start_time"),
          "activity_end_time":  active_tweet.get("activity_end_time"),
          "account_id":       active_tweet.get("account_id")
        }

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    promoted_tweet_id = stream_slice.get("promoted_tweet_id") if stream_slice else None
    return {
      "entity": "PROMOTED_TWEET",
      "entity_ids": promoted_tweet_id,
      "granularity": "TOTAL",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "ENGAGEMENT",
      "start_time": (datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)).strftime(DATE_FORMAT),
      "end_time": datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime(DATE_FORMAT)
    }

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' in response.json():
      data = response.json()['data']
      logger.info(data)
      for record in data:
        id_data = record.get("id_data", [])
        for data_point in id_data:
          metrics = data_point.get("metrics", {})
          engagement_data = {
            "id": stream_slice.get("promoted_tweet_id"),
            "activity_start_time": stream_slice.get("activity_start_time"),
            "activity_end_time": stream_slice.get("activity_end_time"),
            "impressions": None if metrics.get("impressions") == None else metrics.get("impressions")[0],
            "likes": None if  metrics.get("likes") == None else metrics.get("likes")[0],
            "engagements": None if  metrics.get("engagements") == None else metrics.get("engagements")[0],
            "clicks": None if metrics.get("clicks") == None else metrics.get("clicks")[0],
            "retweets": None if metrics.get("retweets") == None else metrics.get("retweets")[0],
            "replies": None if metrics.get("replies") == None else metrics.get("replies")[0],
            "follows": None if metrics.get("follows") == None else metrics.get("follows")[0],
            "app_clicks": None if metrics.get("app_clicks") == None else metrics.get("app_clicks")[0],
            "card_engagements": None if metrics.get("card_engagements") == None else metrics.get("card_engagements")[0],
            "qualified_impressions": None if metrics.get("qualified_impressions") == None else metrics.get("qualified_impressions")[0],
            "tweets_send": None if metrics.get("tweets_send") == None else metrics.get("tweets_send")[0],
            "poll_card_vote": None if metrics.get("poll_card_vote") == None else metrics.get("poll_card_vote")[0],
            "carousel_swipes": None if metrics.get("carousel_swipes") == None else metrics.get("carousel_swipes")[0],
            "account_id": stream_slice['account_id']
          }
          yield engagement_data
      time.sleep(2)
