from typing import Any, Iterable, Mapping, MutableMapping, Optional
import logging
import requests
import time
from datetime import datetime
from .tweets_stream import TweetPromoted

logger = logging.getLogger("airbyte")

class PromotedTweetBilling(TweetPromoted):
    #start_time and account_id  are set in the tweets_stream file 
    url_base = "https://ads-api.x.com/9/"
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"stats/jobs/accounts/{self.account_id}"

    def stream_slices(
        self, 
        sync_mode = None, # will inherit from TweetPromoted
        stream_state: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # Reuse TweetPromoted's stream_slices to get promoted tweets
        for slice in super().stream_slices(stream_state=stream_state, **kwargs):
            tweet = slice.get("parent", {})
            yield {
                "promoted_tweet_id": tweet.get("id"),
               # "created_at": tweet.get("created_at")
            }

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        promoted_tweet_id = stream_slice.get("promoted_tweet_id") if stream_slice else None
        
        params = {
            "entity": "PROMOTED_TWEET",
            "entity_ids": promoted_tweet_id,
            "granularity": "DAY",
            "placement": "ALL_ON_TWITTER",
            "metric_groups": "BILLING",
            "start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
            
        return params

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
                  #  "created_at": stream_slice.get("created_at"),
                  #  "stats_date": record.get("date"),
                    "billed_engagements": record.get("billed_engagements", []),
                    "billed_charge_local_micro": record.get("billed_charge_local_micro", []),
                    **record
                }
                yield billing_data
                
        time.sleep(2) 