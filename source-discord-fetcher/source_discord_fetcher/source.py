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

logger = logging.getLogger("airbyte")

GUILD_KEYS = ["id", "name", "owner_id", "roles", "description", "chain", "max_members"]
CHANNEL_KEYS = ["id", "type", "guild_id", "position", "name", "topic", "last_message_id", "managed", "parent_id", "last_pin_timestamp", "message_count", "member_count", "falgs", "total_message_sent"]
USER_KEYS = [ "id", "username", "discriminator", "global_name", "bot", "mfa_enabled", "verified", "email", "premium_type", "public_flags"]
# Basic full refresh stream
class DiscordFetcherStream(HttpStream, ABC):
    # TODO: Fill in the url base. Required.
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
    primary_key = "guild_id"

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        guild = { key : data.get(key) for key in GUILD_KEYS }
        yield guild 


class GuildChannel(DiscordFetcherStream):
    primary_key="channel_id"

    use_cache=True

    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        #Fixme For some reason the HttpSubstream provoke a call to the GuildChannel endpoint that return a single elt
        # Ignore this call for the meantime
        if type(data) is dict:
            logger.info("Weird case due to the Substream")
            return
        for elt in data:
            channel = { key : elt.get(key) for key in CHANNEL_KEYS }
            yield channel 


class Channel(HttpSubStream, GuildChannel):
    primary_key="channel_id"
    def __init__(self,**kwargs):
        super().__init__(GuildChannel(**kwargs),**kwargs)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        logger.info("Parent: %s", stream_slice.get('parent'))
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
    primary_key="member_id"

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        return {"limit": 1000}


    def parse_response(
        self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs
    ) -> Iterable[Mapping]:
        logger.debug("Response: %s", response.json())
        data=response.json()
        for elt in data:
            user = { key : elt.get('user').get(key) for key in USER_KEYS }
            user['guild_id']=stream_slice['guild_id']
            yield user 

# Source
class SourceDiscordFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
       return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"], auth_method="Bot")
        return [
            Guild(guilds_id=config["guilds_id"],  authenticator=auth),
            GuildChannel(guilds_id=config["guilds_id"], endpoint="/channels", authenticator=auth),
            Channel(guilds_id=config["guilds_id"], authenticator=auth),
            Member(guilds_id=config["guilds_id"], endpoint="/members", authenticator=auth)
        ]
