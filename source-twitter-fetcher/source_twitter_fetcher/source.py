from abc import ABC
from typing import Any, List, Mapping, Tuple
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .tweets_stream import Account, Tweet, TweetMetrics
from .tweets_comments_stream import TweetComments
from .spaces_stream import Space, GetSpaceIds
from .tags_stream import TagsStream
from .auth import TwitterOAuth

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class SourceTwitterFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TwitterOAuth(
            config,
            token_refresh_endpoint="https://api.x.com/2/oauth2/token"
        )

        # Parse start_time if provided, otherwise streams will use their defaults
        start_time = None
        if "start_time" in config:
            start_time = datetime.strptime(config['start_time'], DATE_FORMAT)

        tweet_kwargs = {
            "authenticator": auth,
            "account_id": config["account_id"]
        }

        tags_kwargs = {
            "authenticator": auth,
            "account_id": config["account_id"],
            "tags": config["tags"]
        }
        
        tweet = Tweet(**tweet_kwargs)
        
        tweet_metrics_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": tweet
        }
        
        tweet_comments_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": tweet
        }
        
        # Add start_time only if provided in config
        if start_time:
            tweet_kwargs["start_time"] = start_time
            tags_kwargs["start_time"] = start_time
            tweet_metrics_kwargs["start_time"] = start_time
            tweet_comments_kwargs["start_time"] = start_time
            
        # Add tags_frequent_extractions if provided in config
        if "tags_frequent_extractions" in config:
            tags_kwargs["tags_frequent_extractions"] = config["tags_frequent_extractions"]
            
        tags = TagsStream(**tags_kwargs)
        tweet_metrics = TweetMetrics(**tweet_metrics_kwargs)   
        tweet_comments = TweetComments(**tweet_comments_kwargs)
       
        # Get space IDs from config, no default
        space_ids = config.get("space_ids", [])
        
        space_kwargs = {
            "authenticator": auth,
            "space_ids": space_ids
        }
            
        space = Space(**space_kwargs)

        # Get space account IDs from config for space discovery
        space_account = config.get("space_account", [])
        
        get_space_ids_kwargs = {
            "authenticator": auth,
            "space_account": space_account
        }
        
        get_space_ids = GetSpaceIds(**get_space_ids_kwargs)

        streams = [
            Account(authenticator=auth, account_id=config["account_id"]),
            tweet,
            tweet_metrics,
            tweet_comments,
            space,
            tags,
            get_space_ids
        ]
        return streams
