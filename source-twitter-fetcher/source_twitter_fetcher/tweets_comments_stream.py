from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from .tweets_stream import Tweet
import json
import os

logger = logging.getLogger("airbyte")

class TweetComments(HttpSubStream, Tweet):
    primary_key = "id"
    cursor_field = "created_at"
    
    def __init__(self, start_time: str = None, comment_days_limit: int = 2, filtered_author_ids: List[str] = None, **kwargs):
        super().__init__(start_time=start_time, **kwargs)
        self.comment_days_limit = comment_days_limit
        self.limit_date = datetime.now() - timedelta(days=self.comment_days_limit)
        # Use provided filtered_author_ids or default to empty list
        self.filtered_author_ids = filtered_author_ids or []

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Handle pagination for Twitter search API"""
        response_json = response.json()
        if 'meta' in response_json and 'next_token' in response_json['meta'] and response_json['meta'].get('result_count', 0) > 0:
            return response_json['meta']['next_token']
        return None

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        """
        Return the latest state by comparing the cursor value in the latest record with existing state
        """
        latest_created_at = latest_record.get(self.cursor_field)
        current_state = current_stream_state.get(self.cursor_field)

        if current_state is None:
            return {self.cursor_field: latest_created_at}
        
        return {self.cursor_field: max(latest_created_at, current_state)}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "tweets/search/recent"

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "query": f"conversation_id:{stream_slice['tweet_id']}",
            "tweet.fields": "author_id,created_at,conversation_id,in_reply_to_user_id,referenced_tweets,source,text,public_metrics,entities,context_annotations",
            "user.fields": "created_at,description,id,name,username",
            "expansions": "author_id,referenced_tweets.id",
        }
        if next_page_token:
            params["next_token"] = next_page_token
        return params

    def stream_slices(
        self,
        sync_mode: Any,
        cursor_field: Optional[List[str]] = None,
        stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        # Get tweet IDs from the parent Tweet stream
        for parent_slice in super().stream_slices(sync_mode=sync_mode):
            tweet = parent_slice["parent"]
            yield {"tweet_id": tweet.get('id')}

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        if 'data' in response.json():
            data = response.json()['data']
            for tweet in data:
                # Skip tweets from filtered author IDs or containing "RT"
                if (tweet.get('author_id') not in self.filtered_author_ids and 
                    not tweet.get('text', '').startswith('RT')): #filter out Retweets
                    # Check if the tweet is within the time limit
                    tweet_date = datetime.strptime(tweet.get('created_at'), "%Y-%m-%dT%H:%M:%S.%fZ")
                    if tweet_date >= self.limit_date:
                        # Add account_id to the tweet data
                        tweet['account_id'] = self.account_id
                        yield tweet
        # Add rate limiting delay like other Twitter streams
        time.sleep(2) 