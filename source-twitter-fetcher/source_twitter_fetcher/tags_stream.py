from typing import Any, Iterable, Mapping, MutableMapping, Optional, List
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")

class TwitterStream(HttpStream):
    url_base = "https://api.x.com/2/"
    
    def __init__(self, start_time: str = None, account_id: str = None, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.account_id = account_id
    
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        # Handles API rate limiting with Retry-After headers
        
    def _apply_rate_limiting(self):
        time.sleep(2)  # Rate limiting protection

class TagsStream(TwitterStream):  # Inherit from TwitterStream
    def __init__(self, tags: List[str] = None, tags_frequent_extractions: bool = False, **kwargs):
        super().__init__(**kwargs)  # Let TwitterStream handle start_time, account_id
        
        # Handle tags-specific logic
        if not self.start_time:  # Only override if TwitterStream didn't set it
            if tags_frequent_extractions:
                self.start_time = datetime.utcnow() - timedelta(hours=1, minutes=15)
            else:
                self.start_time = datetime.utcnow() - timedelta(days=5)
                
        self.tags = tags or []
    
 