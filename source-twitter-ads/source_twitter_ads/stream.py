from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import time
from datetime import datetime, timedelta

import requests
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream

logger = logging.getLogger("airbyte")

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_DAY = "%Y-%m-%d"
STATS_ENTITY_BATCH_SIZE = 20


def _utc_midnight() -> datetime:
  """Today at 00:00:00 UTC, naive datetime (Twitter Ads expects naive ISO + Z)."""
  now = datetime.utcnow()
  return datetime(now.year, now.month, now.day)


class TwitterAdsStream(HttpStream):
  url_base = "https://ads-api.x.com/12/"

  def __init__(self, account_ids: List[str] = None, **kwargs):
    super().__init__(**kwargs)
    self.account_ids = account_ids or []
    logger.info("twitter ads account ids: %s", self.account_ids)

  @staticmethod
  def _safe_json(response: requests.Response) -> Optional[Mapping[str, Any]]:
    try:
      return response.json()
    except ValueError:
      logger.warning(
        "Twitter Ads returned a non-JSON body (status=%s, len=%s): %r",
        response.status_code,
        len(response.content or b""),
        (response.text or "")[:200],
      )
      return None

  def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
    body = self._safe_json(response)
    cursor = body.get("next_cursor") if isinstance(body, Mapping) else None
    return {"cursor": cursor} if cursor else None

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
  ) -> MutableMapping[str, Any]:
    params: MutableMapping[str, Any] = {}
    if next_page_token and next_page_token.get("cursor"):
      params["cursor"] = next_page_token["cursor"]
    return params

  def backoff_time(self, response: requests.Response) -> Optional[float]:
    """
    Twitter Ads sends `x-rate-limit-reset` (Unix epoch seconds) on 429s rather
    than `Retry-After`. Without this override the CDK falls back to 5/10/20/...s
    exponential backoff, which keeps retrying long before the 15-minute window
    actually resets and just burns retry attempts on the same wall.
    """
    headers = response.headers
    logger.warning(
      "API rate limit hit (limit=%s, remaining=%s, reset=%s)",
      headers.get("x-rate-limit-limit"),
      headers.get("x-rate-limit-remaining"),
      headers.get("x-rate-limit-reset"),
    )
    reset_at = headers.get("x-rate-limit-reset")
    if reset_at:
      try:
        wait = int(reset_at) - int(time.time())
      except ValueError:
        wait = None
      if wait and wait > 0:
        # Add a couple of seconds of slack so we don't race the reset.
        return float(wait + 2)
    retry_after = headers.get("Retry-After")
    if retry_after:
      try:
        return float(retry_after)
      except ValueError:
        return None
    return None


class Account(TwitterAdsStream):
  primary_key = "id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(self, **kwargs) -> str:
    return "accounts"

  def request_params(self, **kwargs) -> MutableMapping[str, Any]:
    params = super().request_params(**kwargs)
    params.update({"count": 1000, "sort_by": "created_at-desc"})
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" in body:
      for account in body["data"]:
        yield account
    else:
      logger.warn("No data in the account response")


class FundingInstrument(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    account_id = stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/funding_instruments"

  def request_params(self, **kwargs) -> MutableMapping[str, Any]:
    params = super().request_params(**kwargs)
    params.update({"count": 1000, "sort_by": "created_at-desc"})
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" not in body:
      account_name = stream_slice.get("parent", {}).get("name")
      logger.warn("No data in the funding_instrument response for %s account", account_name)
      return
    account_id = stream_slice.get("parent", {}).get("id")
    for instrument in body["data"]:
      instrument["account_id"] = account_id
      yield instrument


class Campaign(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    account_id = stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/campaigns"

  def request_params(self, **kwargs) -> MutableMapping[str, Any]:
    params = super().request_params(**kwargs)
    params.update({"count": 1000, "sort_by": "created_at-desc"})
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" not in body:
      account_name = stream_slice.get("parent", {}).get("name")
      logger.warn("No data in the campaign response for %s account", account_name)
      return
    account_id = stream_slice.get("parent", {}).get("id")
    for campaign in body["data"]:
      campaign["account_id"] = account_id
      yield campaign


class AdvertisementCampaign(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    account_id = stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/line_items"

  def request_params(self, **kwargs) -> MutableMapping[str, Any]:
    params = super().request_params(**kwargs)
    params.update({"count": 1000, "sort_by": "created_at-desc"})
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" in body:
      account_id = stream_slice.get("parent", {}).get("id")
      for line_item in body["data"]:
        line_item["account_id"] = account_id
        yield line_item
    else:
      account_name = stream_slice.get("parent").get("name")
      logger.warn("No data in the line_item response for %s account", account_name)


class PromotedTweet(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  @property
  def use_cache(self) -> bool:
    return True

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    account_id = stream_slice.get("parent").get("id")
    return f"accounts/{account_id}/promoted_tweets"

  def request_params(self, **kwargs) -> MutableMapping[str, Any]:
    params = super().request_params(**kwargs)
    # `count=1000` is the API max; `sort_by=created_at-desc` makes the first
    # page the freshest tweets so partial syncs land the live ones first.
    params.update({"count": 1000, "sort_by": "created_at-desc"})
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" not in body:
      return
    account_id = stream_slice.get("parent", {}).get("id")
    for tweet in body["data"]:
      tweet["account_id"] = account_id
      yield tweet


def _batched_active_tweet_slices(
  parent_iter: Iterable[Mapping[str, Any]],
  batch_size: int = STATS_ENTITY_BATCH_SIZE,
) -> Iterable[Mapping[str, Any]]:
  buffers: MutableMapping[str, List[str]] = {}
  for parent_slice in parent_iter:
    tweet = parent_slice["parent"]
    if tweet.get("entity_status") != "ACTIVE":
      continue
    if tweet.get("paused"):
      continue
    account_id = tweet.get("account_id")
    promoted_tweet_id = tweet.get("id")
    if not account_id or not promoted_tweet_id:
      continue
    buf = buffers.setdefault(account_id, [])
    buf.append(promoted_tweet_id)
    if len(buf) >= batch_size:
      yield {"account_id": account_id, "promoted_tweet_ids": buf}
      buffers[account_id] = []
  for account_id, buf in buffers.items():
    if buf:
      yield {"account_id": account_id, "promoted_tweet_ids": buf}


class PromotedTweetBilling(HttpSubStream, TwitterAdsStream):
  primary_key = "id"

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    return f"stats/accounts/{stream_slice['account_id']}"

  def stream_slices(
    self,
    sync_mode=None,
    stream_state: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Optional[Mapping[str, Any]]]:
    yield from _batched_active_tweet_slices(super().stream_slices(sync_mode=sync_mode))

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
  ) -> MutableMapping[str, Any]:
    params = TwitterAdsStream.request_params(
      self,
      next_page_token=next_page_token,
      stream_state=stream_state,
      stream_slice=stream_slice,
    )
    end = _utc_midnight()
    params.update({
      "entity": "PROMOTED_TWEET",
      "entity_ids": ",".join(stream_slice["promoted_tweet_ids"]),
      "granularity": "DAY",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "BILLING",
      # Use a bare YYYY-MM-DD here; see DATE_FORMAT_DAY note above.
      "start_time": (end - timedelta(days=7)).strftime(DATE_FORMAT_DAY),
      "end_time": end.strftime(DATE_FORMAT_DAY),
    })
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" not in body:
      return
    # The request uses the same window/placement/granularity for every slice,
    # so we mirror them onto each row instead of leaving the columns null.
    request_end_date = _utc_midnight().date().isoformat()
    for record in body["data"]:
      # When entity_ids is comma-separated, Twitter returns one record per id
      # in `data[]`; the entity is on `record["id"]`, not on the slice.
      entity_id = record.get("id")
      for data_point in record.get("id_data", []):
        metrics = data_point.get("metrics") or {}
        yield {
          "id": entity_id,
          "account_id": stream_slice["account_id"],
          "activity_start_time": None,
          "activity_end_time": None,
          "billed_engagements": metrics.get("billed_engagements") or [],
          "billed_charge_local_micro": metrics.get("billed_charge_local_micro") or [],
          "placement": "ALL_ON_TWITTER",
          "granularity": "DAY",
          "date": request_end_date,
        }
    time.sleep(1)


class PromotedTweetEngagement(HttpSubStream, TwitterAdsStream):
  """Daily engagement metrics per promoted tweet from /stats with granularity=DAY.
  """

  primary_key = "id"

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

  def path(
    self,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
    next_page_token: Mapping[str, Any] = None,
  ) -> str:
    return f"stats/accounts/{stream_slice['account_id']}"

  def stream_slices(
    self,
    sync_mode=None,
    stream_state: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Optional[Mapping[str, Any]]]:
    yield from _batched_active_tweet_slices(super().stream_slices(sync_mode=sync_mode))

  def request_params(
    self,
    next_page_token: Optional[Mapping[str, Any]] = None,
    stream_state: Mapping[str, Any] = None,
    stream_slice: Mapping[str, Any] = None,
  ) -> MutableMapping[str, Any]:
    params = TwitterAdsStream.request_params(
      self,
      next_page_token=next_page_token,
      stream_state=stream_state,
      stream_slice=stream_slice,
    )
    end = _utc_midnight()
    params.update({
      "entity": "PROMOTED_TWEET",
      "entity_ids": ",".join(stream_slice["promoted_tweet_ids"]),
      "granularity": "DAY",
      "placement": "ALL_ON_TWITTER",
      "metric_groups": "ENGAGEMENT",
      # Use a bare YYYY-MM-DD here; see DATE_FORMAT_DAY note above.
      "start_time": (end - timedelta(days=7)).strftime(DATE_FORMAT_DAY),
      "end_time": end.strftime(DATE_FORMAT_DAY),
    })
    return params

  def parse_response(
    self,
    response: requests.Response,
    stream_slice: Mapping[str, Any] = None,
    **kwargs,
  ) -> Iterable[Mapping]:
    body = self._safe_json(response) or {}
    if "data" not in body:
      return
    request_end_date = _utc_midnight().date().isoformat()
    for record in body["data"]:
      entity_id = record.get("id")
      for data_point in record.get("id_data", []):
        metrics = data_point.get("metrics") or {}
        row = {
          "id": entity_id,
          "account_id": stream_slice["account_id"],
          "activity_start_time": None,
          "activity_end_time": None,
          "placement": "ALL_ON_TWITTER",
          "granularity": "DAY",
          "date": request_end_date,
        }
        for metric in self.ENGAGEMENT_METRICS:
          row[metric] = metrics.get(metric) or []
        yield row
    time.sleep(2)
