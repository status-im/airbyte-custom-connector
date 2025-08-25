from abc import ABC
from typing import Any, List, Mapping, Tuple
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .tweets_stream import Account, Tweet, TweetMetrics, TweetPromoted
from .tweets_comments_stream import TweetComments
from .ads_stream import PromotedTweetActive, PromotedTweetBilling, PromotedTweetEngagement
from .spaces_stream import Space
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
        if start_time:
            tweet_kwargs["start_time"] = start_time
            
        tweet = Tweet(**tweet_kwargs)

        tags_kwargs = {
            "authenticator": auth,
            "account_id": config["account_id"],
            "tags": config["tags"]
        }
        
        # Add start_time only if provided in config
        if start_time:
            tags_kwargs["start_time"] = start_time
            
        # Add tags_frequent_extractions if provided in config
        if "tags_frequent_extractions" in config:
            tags_kwargs["tags_frequent_extractions"] = config["tags_frequent_extractions"]
            
        tags = TagsStream(**tags_kwargs)

        tweet_metrics_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": tweet
        }
        if start_time:
            tweet_metrics_kwargs["start_time"] = start_time
            
        tweet_metrics = TweetMetrics(**tweet_metrics_kwargs)

        tweet_promoted_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": tweet
        }
        if start_time:
            tweet_promoted_kwargs["start_time"] = start_time
            
        tweet_promoted = TweetPromoted(**tweet_promoted_kwargs)

        tweet_comments_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": tweet
        }
        if start_time:
            tweet_comments_kwargs["start_time"] = start_time
            
        tweet_comments = TweetComments(**tweet_comments_kwargs)

        promoted_tweet_active_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id']
        }
        if start_time:
            promoted_tweet_active_kwargs["start_time"] = start_time
            
        promoted_tweet_active = PromotedTweetActive(**promoted_tweet_active_kwargs)

        promoted_tweet_billing_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": promoted_tweet_active
        }
        if start_time:
            promoted_tweet_billing_kwargs["start_time"] = start_time
            
        promoted_tweet_billing = PromotedTweetBilling(**promoted_tweet_billing_kwargs)

        promoted_tweet_engagement_kwargs = {
            "authenticator": auth,
            "account_id": config['account_id'],
            "parent": promoted_tweet_active
        }
        if start_time:
            promoted_tweet_engagement_kwargs["start_time"] = start_time
            
        promoted_tweet_engagement = PromotedTweetEngagement(**promoted_tweet_engagement_kwargs)

        # Get space IDs from config, no default
        space_ids = config.get("space_ids", [])
        
        space_kwargs = {
            "authenticator": auth,
            "space_ids": space_ids
        }
        # Space stream doesn't use start_time since it fetches specific spaces
            
        space = Space(**space_kwargs)

        return [
            Account(authenticator=auth, account_id=config["account_id"]),
            tweet,
            tweet_metrics,
            tweet_promoted,
            tweet_comments,
            promoted_tweet_active,
            promoted_tweet_billing,
            promoted_tweet_engagement,
            space,
            tags
        ]
