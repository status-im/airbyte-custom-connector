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


class PromotedTweet(HttpSubStream, TwitterAdsStream):
  # Maps promoted-tweet entity_id (used by the stats endpoint) to the real tweet_id
  # (used by the public tweets API). Needed to join paid spend back to organic tweets.
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None
  ) -> str:
    account_id = stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/promoted_tweets"

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' not in response.json():
      account_name = stream_slice.get("parent").get("name")
      logger.warn("No data in the promoted_tweets response for %s account", account_name)
      return
    account_id = stream_slice.get("parent").get("id")
    for promoted_tweet in response.json()['data']:
      promoted_tweet["account_id"] = account_id
      yield promoted_tweet


# Window-based stats streams (billing + engagement) fetch the last 7 days of daily data.
# Twitter's billing is a sliding 7-day window and amounts can be revised, so we re-pull
# and let downstream dedup on (account_id, id, date).
STATS_WINDOW_DAYS = 7


def _stats_window(now: datetime = None):
  """Compute the (start, end) window for the stats endpoint.

  Returns a tuple of (start_datetime, end_datetime) where both are midnight-aligned,
  end is today, and start is STATS_WINDOW_DAYS before end. Computed once per slice so
  request_params and parse_response see identical values even if the sync crosses midnight.
  """
  today = (now or datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
  return today - timedelta(days=STATS_WINDOW_DAYS), today


class _DailyStatsSubStream(HttpSubStream, PromotedTweetActive):
  """Shared behaviour for daily stats substreams (billing, engagement).

  Each slice carries the window dates so parse_response can derive a `date` per day
  without recomputing `now()`.
  """

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
    window_start, window_end = _stats_window()
    for parent_slice in super().stream_slices(sync_mode=sync_mode):
      active_tweet = parent_slice["parent"]
      if "ALL_ON_TWITTER" in active_tweet.get("placements", []):
        yield {
          "account_id": active_tweet["account_id"],
          "promoted_tweet_id": active_tweet.get("entity_id"),
          "activity_start_time": active_tweet.get("activity_start_time"),
          "activity_end_time": active_tweet.get("activity_end_time"),
          "window_start_date": window_start.strftime(DATE_FORMAT_2),
          "window_end_date": window_end.strftime(DATE_FORMAT_2),
        }


class PromotedTweetBilling(_DailyStatsSubStream):
  # Daily billing metrics per promoted tweet (one row per tweet per day in the window).
  primary_key = ["id", "date"]

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return {
      "entity": "PROMOTED_TWEET",
      "entity_ids": stream_slice.get("promoted_tweet_id"),
      "granularity": "DAY",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "BILLING",
      "start_time": stream_slice["window_start_date"],
      "end_time": stream_slice["window_end_date"],
    }

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' not in response.json():
      return
    start_day = datetime.strptime(stream_slice["window_start_date"], DATE_FORMAT_2).date()
    for record in response.json()['data']:
      for data_point in record.get("id_data", []):
        metrics = data_point.get("metrics") or {}
        engagements = metrics.get("billed_engagements") or []
        charges = metrics.get("billed_charge_local_micro") or []
        # Twitter returns one element per day in the requested window; zip() guards
        # against the rare case where the two arrays have different lengths.
        for i, (engagement, charge) in enumerate(zip(engagements, charges)):
          yield {
            "id": stream_slice["promoted_tweet_id"],
            "account_id": stream_slice["account_id"],
            "activity_start_time": stream_slice.get("activity_start_time"),
            "activity_end_time": stream_slice.get("activity_end_time"),
            "date": (start_day + timedelta(days=i)).strftime(DATE_FORMAT_2),
            "placement": "ALL_ON_TWITTER",
            "granularity": "DAY",
            "billed_engagements": engagement,
            "billed_charge_local_micro": charge,
          }
    time.sleep(1)


class PromotedTweetEngagement(_DailyStatsSubStream):
  # Daily engagement metrics per promoted tweet (one row per tweet per day in the window).
  primary_key = ["id", "date"]

  # Metrics returned by the stats endpoint under `ENGAGEMENT`. Each metric comes back as
  # an array of length N (one value per day) when granularity=DAY.
  ENGAGEMENT_METRICS = (
    "impressions",
    "likes",
    "engagements",
    "clicks",
    "retweets",
    "replies",
    "follows",
    "app_clicks",
    "card_engagements",
    "qualified_impressions",
    "tweets_send",
    "poll_card_vote",
    "carousel_swipes",
  )

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None
  ) -> MutableMapping[str, Any]:
    return {
      "entity": "PROMOTED_TWEET",
      "entity_ids": stream_slice.get("promoted_tweet_id"),
      "granularity": "DAY",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "ENGAGEMENT",
      # The stats endpoint expects ISO datetimes (not plain dates) for engagement pulls.
      "start_time": f"{stream_slice['window_start_date']}T00:00:00Z",
      "end_time": f"{stream_slice['window_end_date']}T00:00:00Z",
    }

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs
  ) -> Iterable[Mapping]:
    if 'data' not in response.json():
      return
    start_day = datetime.strptime(stream_slice["window_start_date"], DATE_FORMAT_2).date()
    for record in response.json()['data']:
      for data_point in record.get("id_data", []):
        metrics = data_point.get("metrics") or {}
        # Every metric is either an array of daily values or None. Use the longest
        # array to size the day loop so we don't drop days when one metric is None.
        day_count = max(
          (len(metrics.get(m) or []) for m in self.ENGAGEMENT_METRICS),
          default=0,
        )
        for i in range(day_count):
          row = {
            "id": stream_slice["promoted_tweet_id"],
            "account_id": stream_slice["account_id"],
            "activity_start_time": stream_slice.get("activity_start_time"),
            "activity_end_time": stream_slice.get("activity_end_time"),
            "date": (start_day + timedelta(days=i)).strftime(DATE_FORMAT_2),
            "placement": "ALL_ON_TWITTER",
            "granularity": "DAY",
          }
          for metric in self.ENGAGEMENT_METRICS:
            values = metrics.get(metric)
            row[metric] = values[i] if values and i < len(values) else None
          yield row
    time.sleep(2)
