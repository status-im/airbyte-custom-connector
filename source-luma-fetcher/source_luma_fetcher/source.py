#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

# Luma Events stream
class LumaEventsStream(HttpStream):
    url_base = "https://public-api.luma.com/v1/"
    primary_key = None

    def __init__(self, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if isinstance(data, dict) and data.get('has_more') and data.get('next_cursor'):
            return {"cursor": data['next_cursor']}
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "accept": "application/json",
            "x-luma-api-key": self.api_key
        }

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "calendar/list-events"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        if next_page_token and next_page_token.get('cursor'):
            params['cursor'] = next_page_token['cursor']
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        # Handle the Luma API response structure with 'entries'
        if isinstance(data, dict) and 'entries' in data:
            for entry in data['entries']:
                yield entry
        # Fallback for other response formats
        elif isinstance(data, list):
            for event in data:
                yield event
        elif isinstance(data, dict):
            if 'events' in data:
                for event in data['events']:
                    yield event
            elif 'data' in data:
                for event in data['data']:
                    yield event
            else:
                # If it's a single object, yield it
                yield data

# Source
class SourceLumaFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            # Test the connection by making a request to the API
            api_key = config.get('api_key')
            if not api_key:
                return False, "API key is required"

            headers = {
                "accept": "application/json",
                "x-luma-api-key": api_key
            }

            response = requests.get(
                "https://public-api.luma.com/v1/calendar/list-events",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                return True, None
            else:
                return False, f"API request failed with status {response.status_code}: {response.text}"

        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        api_key = config['api_key']
        return [LumaEventsStream(api_key=api_key)]
