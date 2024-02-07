#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

# Basic full refresh stream
class ApiStream(HttpStream):
    url_base =  ""
    primary_key = None

    def __init__(self, api_key, url, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.url = url

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return { "API-Key" : f"{self.api_key}"}

class BankBalance(ApiStream):

    primary_key="account_id" 
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/api/bank"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        for elt in data:
            yield elt

class AdditionalTokens(ApiStream):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/api/tokens"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        for elt in data:
            yield elt

# Source
class SourceBasicApiFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams=[]
        if 'bank' in config['endpoints']:
            streams.append(BankBalance(config['api_key'], config['url']))
        if 'tokens' in config['endpoints']:
            streams.append(AdditionalTokens(config['api_key'],config['url']))
        return streams
