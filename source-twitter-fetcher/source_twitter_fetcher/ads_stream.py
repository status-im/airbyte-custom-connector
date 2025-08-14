from typing import Any, Iterable, Mapping, MutableMapping, Optional, Union
import logging
import requests
import time
from datetime import datetime
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from .tweets_stream import TwitterStream

logger = logging.getLogger("airbyte")

class PromotedTweetActive(TwitterStream):
   #fetch the active promoted twwet ids
    url_base = "https://ads-api.x.com/12/"
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
        return f"stats/accounts/{self.account_id}/active_entities"

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "entity": "PROMOTED_TWEET",
            "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ") 
        }

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        if 'data' in response.json():
            for entity in response.json()['data']:
                yield entity
        time.sleep(2)


class PromotedTweetBilling(HttpSubStream, PromotedTweetActive):
    #gets billing info for each promotted tweet 
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, **kwargs):
        super().__init__(start_time=start_time, **kwargs)

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"stats/accounts/{self.account_id}"

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
            "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
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
                    **record
                }
                yield billing_data
                
            time.sleep(2)

class PromotedTweetEngagement(HttpSubStream, PromotedTweetActive):
    # fetches engagement metrics on promoted tweets
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, **kwargs):
        super().__init__(start_time=start_time, **kwargs)

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"stats/accounts/{self.account_id}"

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
            "granularity": "TOTAL",
            "placement": "ALL_ON_TWITTER",
            "metric_groups": "ENGAGEMENT",
            "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
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
                id_data = record.get("id_data", [])
                for data_point in id_data:
                    metrics = data_point.get("metrics", {})
                    engagement_data = {
                        "id": stream_slice.get("promoted_tweet_id"),
                        "activity_start_time": stream_slice.get("activity_start_time"),
                        "activity_end_time": stream_slice.get("activity_end_time"),
                        "impressions": metrics.get("impressions", [None])[0],
                        "likes": metrics.get("likes", [None])[0],
                        "engagements": metrics.get("engagements", [None])[0],
                        "clicks": metrics.get("clicks", [None])[0],
                        "retweets": metrics.get("retweets", [None])[0],
                        "replies": metrics.get("replies", [None])[0],
                        "follows": metrics.get("follows", [None])[0],
                        "app_clicks": metrics.get("app_clicks", [None])[0],
                        "card_engagements": metrics.get("card_engagements", [None])[0],
                        "qualified_impressions": metrics.get("qualified_impressions", [None])[0],
                        "tweets_send": metrics.get("tweets_send", [None])[0],
                        "poll_card_vote": metrics.get("poll_card_vote", [None])[0],
                        "carousel_swipes": metrics.get("carousel_swipes", [None])[0],
                    }
                    yield engagement_data
                
            time.sleep(2)