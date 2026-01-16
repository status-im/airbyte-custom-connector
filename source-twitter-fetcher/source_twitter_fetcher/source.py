from abc import ABC
from typing import Any, List, Mapping, Tuple
from datetime import datetime
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .tweets_stream import Account, AccountsAdditional, Tweet, TweetMetrics
from .tweets_comments_stream import TweetComments
from .spaces_stream import Space, GetSpaceIds
from .tags_stream import Tags
from .auth import TwitterOAuth, TwitterBearerTokenAuth
import logging

logger = logging.getLogger("airbyte")
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class SourceTwitterFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # 3-legged OAuth for user-context streams
        auth = TwitterOAuth(
            config,
            token_refresh_endpoint="https://api.x.com/2/oauth2/token"
        )

        # Bearer Token authentication for public data streams (tags and space discovery)
        bearer_auth = TwitterBearerTokenAuth(config)

        # Parse start_time if provided, otherwise streams will use their defaults
        start_time = None
        if "start_time" in config:
            start_time = datetime.strptime(config["start_time"], DATE_FORMAT)

        kwargs = {
            "authenticator": auth,
            "account_ids": config["account_ids"]
        }

        tweet = Tweet(**kwargs)

        default_args = {
            "authenticator": auth,
            "parent": tweet,
            "account_ids": config["account_ids"],        }

        tags_kwargs = {
            "authenticator": bearer_auth,
            "account_ids": config["account_ids"],
            "tags": config["tags"]
        }
        # Add start_time only if provided in config
        if start_time:
            kwargs["start_time"] = start_time
            tags_kwargs["start_time"] = start_time
            default_args["start_time"] = start_time

        streams = [
            Account(authenticator=auth, account_ids=config["account_ids"]),
            tweet,
            TweetMetrics(**default_args),
            TweetComments(**default_args)
        ]
        additionals_accounts = config["account_ids"]
        if len(additionals_accounts) > 0:
            streams.append(AccountsAdditional(**kwargs))

        # Add tags_frequent_extractions if provided in config
        if "tags_frequent_extractions" in config:
            tags_kwargs["tags_frequent_extractions"] = config["tags_frequent_extractions"]

        logger.info("Tags in the config : %s", config['tags'])
        tags_list = config['tags']
        logger.info("len %s", len(tags_list))
        if len(tags_list) > 0:
            tags = Tags(**tags_kwargs)
            streams.append(tags)
            logger.info("tags added to the config")
        # Get space IDs from config, no default
        space_ids = config.get("space_ids", [])
        if len(space_ids) > 0:
            space_kwargs = {
                "authenticator": auth,
                "space_ids": space_ids
            }
            space = Space(**space_kwargs)
            streams.append(space)
        # Get space account IDs from config for space discovery
        space_account = config.get("space_account", [])
        if len(space_account) > 0:
            get_space_ids_kwargs = {
                "authenticator": bearer_auth,
                "space_account": space_account
            }
            get_space_ids = GetSpaceIds(**get_space_ids_kwargs)
            streams.append(get_space_ids)
        return streams
