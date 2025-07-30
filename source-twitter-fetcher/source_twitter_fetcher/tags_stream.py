from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

from .tweets_stream import TwitterStream

logger = logging.getLogger("airbyte")

class TagsStream(TwitterStream):
    primary_key = "id"

    def __init__(self, start_time: str = None, account_id: str = None, tags: List[str] = None, tags_frequent_extractions: bool = False, **kwargs):
        super().__init__(start_time=start_time, account_id=account_id, **kwargs)
        
        if not self.start_time:
            if tags_frequent_extractions:
                # Default to 1 hour 15 minutes before current time
                self.start_time = datetime.utcnow() - timedelta(hours=1, minutes=15)
            else:
                # Default to 5 days before current time
                self.start_time = datetime.utcnow() - timedelta(days=5)
            
        self.tags = tags or []

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for tag in self.tags:
            yield {"tag": tag}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "tweets/search/recent" # this endpoint fetches data from the last 7 days 

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if 'meta' in response.json() and 'next_token' in response.json()['meta'] and response.json()['meta']['result_count'] > 0:
            logger.debug('DBG-NT: %s', response.json()['meta']['next_token'])
            return {"next_token": response.json()['meta']['next_token']}

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        tag = stream_slice["tag"]
        params = {
            "query": tag,
            "tweet.fields": "text,public_metrics,author_id,referenced_tweets,created_at",
            "expansions": "author_id",
            "user.fields": "username,name,verified,public_metrics",
            "max_results": 100
        }
        params.update({"start_time": self.start_time.strftime("%Y-%m-%dT%H:%M:%SZ")})
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Full response %s", response.json())
        response_data = response.json()
        
        # Create a mapping of user_id to user info for quick lookup because user data is returned separately in the includes.users array, you need to manually join them using the author_id as the key
        users_map = {}
        if 'includes' in response_data and 'users' in response_data['includes']:
            for user in response_data['includes']['users']:
                users_map[user['id']] = user
        
        if 'data' in response_data:
            data = response_data['data']
            for t in data:
                t["matched_tag"] = stream_slice["tag"]
                
                if t.get('author_id') and t['author_id'] in users_map:
                    user_info = users_map[t['author_id']]
                    t["author_username"] = user_info.get('username')
                    t["author_name"] = user_info.get('name')
                    t["author_verified"] = user_info.get('verified')
                
                yield t
        self._apply_rate_limiting() 

 