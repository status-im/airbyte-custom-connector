from abc import ABC
from typing import Any, List, Mapping, Tuple
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .tweets_stream import Account, Tweet, TweetMetrics, TweetPromoted
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

        tweet = Tweet(
            authenticator=auth,
            account_id=config["account_id"],
            start_time=datetime.strptime(config['start_time'], DATE_FORMAT),
        )

        tags_kwargs = {
            "authenticator": auth,
            "account_id": config["account_id"],
            "tags": config["tags"]
        }
        
        # Add start_time only if provided in config
        if "start_time" in config:
            tags_kwargs["start_time"] = datetime.strptime(config['start_time'], DATE_FORMAT)
            
        tags = TagsStream(**tags_kwargs)

        tweet_metrics = TweetMetrics(
            authenticator=auth,
            account_id=config['account_id'],
            parent=tweet
        )

        tweet_promoted = TweetPromoted(
            authenticator=auth,
            account_id=config['account_id'],
            parent=tweet
        )

        promoted_tweet_active = PromotedTweetActive(
            authenticator=auth,
            account_id=config['account_id'],
            start_time=datetime.strptime(config['start_time'], DATE_FORMAT),
        )

        promoted_tweet_billing = PromotedTweetBilling(
            authenticator=auth,
            account_id=config['account_id'],
            parent=tweet_promoted
        )

        promoted_tweet_engagement = PromotedTweetEngagement(
            authenticator=auth,
            account_id=config['account_id'],
            parent=promoted_tweet_active
        )

        space = Space(
            authenticator=auth,
            account_id=config['account_id'],
            start_time=datetime.strptime(config['start_time'], DATE_FORMAT)
        )

        return [
            Account(authenticator=auth, account_id=config["account_id"]),
            tweet,
            tweet_metrics,
            tweet_promoted,
            promoted_tweet_active,
            promoted_tweet_billing,
            promoted_tweet_engagement,
            space,
            tags
        ]
