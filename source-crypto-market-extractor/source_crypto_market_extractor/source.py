#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime
import requests
import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")

class CoinPrice(HttpStream):
    url_base = 'https://api.coingecko.com/api/v3/coins/'
   
    primary_key = None

    def __init__(self, coins: List['str'],  **kwargs):
        super().__init__(**kwargs)
        self.coins= coins

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for coin in self.coins:
            yield {
                "coin":  coin
            }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['coin']}/market_chart?vs_currency=usd&days=1&interval=daily&precision=18"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        coin=stream_slice["coin"]
        logger.info("Parsing Coin Gecko data for %s", coin)
        market_chart = response.json()
        yield {
            "coin": coin,
            "date": datetime.today().strftime('%Y%m%d_%H%M'),
            # The first value of the prices objects is weird
            "price": market_chart['prices'][1][1],
        }

# Source
class SourceCryptoMarketExtractor(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
       return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        logger.info('Config : %s', config['coins'])
        return [CoinPrice(coins=config['coins'])]
