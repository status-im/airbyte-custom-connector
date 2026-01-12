#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpSubStream, HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
import time
import os
import json
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("airbyte")

GUILD_KEYS = ["id", "name", "owner_id", "roles", "description", "chain", "max_members", "approximate_member_count"]
CHANNEL_KEYS = ["id", "type", "guild_id", "position", "name", "topic", "last_message_id", "managed", "parent_id", "last_pin_timestamp", "message_count", "member_count", "falgs", "total_message_sent"]
USER_KEYS = [ "id", "username", "discriminator", "global_name", "bot", "mfa_enabled", "verified", "email", "premium_type", "public_flags"]
ROLES_KEYS = ["id", "name", "color", "hoist", "position", "permissions", "managed", "mentionable", "falgs", "guild_id"]

MAX_USERS = 1000
# Basic full refresh stream
class DiscordFetcherStream(HttpStream, ABC):
    url_base = "https://discord.com/api/"

    def __init__(self, guilds_id: str, endpoint: str="", **kwargs):
        super().__init__(**kwargs)
        self.guilds_id = guilds_id
        self.endpoint = endpoint

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"guilds/{stream_slice['guild_id']}{self.endpoint}"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for guild_id in self.guilds_id:
            yield {
            "guild_id": guild_id
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
       return None

class Guild(DiscordFetcherStream):
    primary_key = "id"

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        # Request guild with counts to get approximate_member_count
        return {"with_counts": "true"}

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        guild = { key : data.get(key) for key in GUILD_KEYS }
        yield guild


class GuildChannel(DiscordFetcherStream):
    primary_key="id"

    use_cache=True

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("gld_chnl_exec: Response: %s", response.json())
        data=response.json()
        for elt in data:
            channel = { key : elt.get(key) for key in CHANNEL_KEYS }
            yield channel


class Channel(HttpSubStream, DiscordFetcherStream):
    primary_key="id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        logger.info("chnl_exec: Parent: %s", stream_slice.get('parent'))
        channel_id = stream_slice.get('parent').get('id')
        return f"channels/{channel_id}"

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        channel = { key : data.get(key) for key in CHANNEL_KEYS }
        yield channel

class Member(DiscordFetcherStream):
    primary_key="id"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # if the response doesn't contain the maximum number of user then there is no more to fetch
        logger.debug("memb_exec : response size : %s", len(response.json()))
        if len(response.json()) == MAX_USERS:
            last_member_id = response.json()[len(response.json()) - 1].get('user').get('id')
            return {"after": last_member_id}

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {"limit": MAX_USERS}

        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        for elt in data:
            user = { key : elt.get('user').get(key) for key in USER_KEYS }
            user['guild_id']=stream_slice['guild_id']
            user['roles']=elt.get('roles')
            yield user

class GuildRole(DiscordFetcherStream):
    primary_key=None

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        for elt in data:
            logger.info('Role : %s',  elt)
            role = { key : elt.get(key) for key in ROLES_KEYS }
            role['guild_id']=stream_slice['guild_id']
            yield role

class ChannelMessagesStream(DiscordFetcherStream):
    """
    Stream for extracting all messages from multiple Discord channels.
    """

    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(guilds_id=config["guilds_id"], endpoint="/messages", **kwargs)
        self.channel_ids = config["channel_id"]
        # Set default start_date to 4 days before current day if not provided
        if config.get("start_date"):
            self.start_date = config["start_date"]
        else:
            default_date = datetime.now(timezone.utc) - timedelta(days=4)
            self.start_date = default_date.strftime("%Y-%m-%d")
            logger.info("No start_date provided, using default: %s", self.start_date)

    @property
    def name(self) -> str:
        return "channel_messages"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        logger.info("ChannelMessagesStream stream_slices - channel_ids: %s", self.channel_ids)
        for channel_id in self.channel_ids:
            slice_data = {"channel_id": channel_id}
            logger.info("ChannelMessagesStream yielding slice: %s", slice_data)
            yield slice_data

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        logger.info("ChannelMessagesStream path - stream_slice: %s", stream_slice)
        channel_id = stream_slice["channel_id"]
        return f"channels/{channel_id}/messages"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        messages = response.json()
        if messages and len(messages) == 100:  # Discord's max limit
            return {"before": messages[-1]["id"]}
        return None

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {"limit": 100}

        # Only one of before, after, or around can be used at a time
        if next_page_token:
            params.update(next_page_token)
        elif stream_state and "last_message_id" in stream_state:
            params["after"] = stream_state["last_message_id"]
        elif self.start_date:
            # Convert start_date to Discord snowflake ID
            # Discord epoch (2015-01-01) in milliseconds
            DISCORD_EPOCH = 1420070400000
            start_date_ts = int(datetime.strptime(self.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
            # Convert timestamp to snowflake
            snowflake = ((start_date_ts - DISCORD_EPOCH) << 22)
            params["after"] = str(snowflake)

        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        messages = response.json()
        for message in messages:
            # Add channel_id to each message for reference
            message["channel_id"] = stream_slice["channel_id"]
            yield message

# Source
class SourceDiscordFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
       return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"], auth_method="Bot")
        guildChannel=GuildChannel(guilds_id=config["guilds_id"], endpoint="/channels", authenticator=auth)

        # Create base streams
        streams = [
            Guild(guilds_id=config["guilds_id"],  authenticator=auth),
            guildChannel,
            Channel(guilds_id=config["guilds_id"], authenticator=auth, parent=guildChannel),
            Member(guilds_id=config["guilds_id"], endpoint="/members", authenticator=auth),
            GuildRole(guilds_id=config["guilds_id"], endpoint="/roles", authenticator=auth),
            ChannelMessagesStream(config, authenticator=auth),
        ]

        return streams
