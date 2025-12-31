from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime
import requests
import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")


class TokenPrices(HttpStream):
    """Stream to fetch current token prices from Alchemy API."""

    url_base = "https://api.g.alchemy.com/prices/v1/"
    primary_key = ["symbol", "price_last_updated_at"]
    cursor_field = "price_last_updated_at"

    def __init__(self, api_key: str, symbols: List[str], **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.symbols = symbols
        self._cursor_value = None

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        symbols_param = "&".join([f"symbols={symbol}" for symbol in self.symbols])
        return f"tokens/by-symbol?{symbols_param}"

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()

        for token in data.get("data", []):
            prices = token.get("prices", [])
            price_info = prices[0] if prices else {}
            price_last_updated_at = price_info.get("lastUpdatedAt")

            yield {
                "symbol": token.get("symbol"),
                "price_usd": price_info.get("value"),
                "price_last_updated_at": price_last_updated_at,
                "currency": price_info.get("currency"),
            }

            if price_last_updated_at:
                self._cursor_value = price_last_updated_at


class HistoricalTokenPrices(HttpStream):
    """Stream to fetch historical token prices from Alchemy API."""

    name = "token_prices"
    primary_key = ["symbol", "price_last_updated_at"]
    cursor_field = "price_last_updated_at"
    http_method = "POST"

    def __init__(self, api_key: str, symbols: List[str], historical_timestamp: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.symbols = symbols
        self.historical_timestamp = historical_timestamp
        self._cursor_value = None

    @property
    def url_base(self) -> str:
        return f"https://api.g.alchemy.com/prices/v1/{self.api_key}/"

    @property
    def state(self) -> Mapping[str, Any]:
        return {self.cursor_field: self._cursor_value} if self._cursor_value else {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value.get(self.cursor_field)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for symbol in self.symbols:
            yield {"symbol": symbol}

    def path(self, **kwargs) -> str:
        return "tokens/historical"

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        return {"Content-Type": "application/json"}

    def request_body_json(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> Optional[Mapping]:
        return {
            "symbol": stream_slice["symbol"],
            "startTime": self.historical_timestamp,
            "endTime": self.historical_timestamp,
        }

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        response_data = response.json()

        symbol = response_data.get("symbol", stream_slice["symbol"])
        currency = response_data.get("currency")
        prices = response_data.get("data", [])

        if prices:
            price_info = prices[0]
            price_last_updated_at = price_info.get("timestamp")

            yield {
                "symbol": symbol,
                "price_usd": price_info.get("value"),
                "price_last_updated_at": price_last_updated_at,
                "currency": currency,
            }

            if price_last_updated_at:
                self._cursor_value = price_last_updated_at
        else:
            yield {
                "symbol": symbol,
                "price_usd": None,
                "price_last_updated_at": self.historical_timestamp,
                "currency": currency,
            }
            self._cursor_value = self.historical_timestamp


class SourceAlchemyFetcher(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            url = "https://api.g.alchemy.com/prices/v1/tokens/by-symbol?symbols=ETH"
            headers = {"Authorization": f"Bearer {config['api_key']}"}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        historical_fetch = config.get("historical_fetch", False)

        if historical_fetch:
            historical_timestamp = config.get("historical_timestamp")
            if not historical_timestamp:
                raise ValueError("historical_timestamp is required when historical_fetch is enabled")
            return [HistoricalTokenPrices(
                api_key=config["api_key"],
                symbols=config["symbols"],
                historical_timestamp=historical_timestamp,
            )]

        return [TokenPrices(api_key=config["api_key"], symbols=config["symbols"])]
