#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


#from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from .utils import extract_token
import logging
import requests
import json
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")


class BitcoinToken(HttpStream):
    url_base = "https://blockchain.info/rawaddr/"

    primary_key = None

    def __init__(self, wallets: List[str], **kwargs):
        super().__init__(**kwargs)
        self.wallets = wallets

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            yield {
                "address": wallet['address'], 
                "name": wallet['name']
            }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Getting Bitcoin Balance information")
        bitcoin_data = response.json()
        yield {
             "wallet_name": stream_slice['name'],
             "name":"BTC", 
             "symbol":"BTC", 
             "description": "Bitcoin", 
             "address":"", 
             "chain": "bitcoin",
             "balance": bitcoin_data['final_balance'],
             "decimal":8
        }

class EthereumToken(HttpStream):

    url_base = "https://api.ethplorer.io/getAddressInfo/"

    primary_key = None

    def __init__(self, wallets: List[str], **kwargs):
        super().__init__(**kwargs)
        self.wallets = wallets

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            yield {
                "address": wallet['address'], 
                "name": wallet['name']
            }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}?apiKey=freekey"

    def parse_response(self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Getting ETH balance information")
        eth_data=response.json()['ETH']
        yield {
            "wallet_name": stream_slice['name'],
            "name":"ETH", 
            "symbol":"ETH", 
            "description": "Native Ethereum token", 
            "address":"", 
            "chain": "Ethereum",
            "balance":eth_data['rawBalance'],
            "decimal":18
        }
        logging.info("Fetching Tokens balance information")
        tokens_data=response.json()['tokens']
        for t in tokens_data:
            try:
                yield extract_token(stream_slice['name'], t)
            except Exception as e:
                logger.error('Dropping token not valid %s' % t )

# Source
class SourceWalletFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        
        bitcoin_wallets: List = []
        ethereum_wallets: List = []

        for wallet in config['wallets']:
            if 'BTC' in wallet['blockchain']:
                bitcoin_wallets.append(wallet)
            if 'ETH' in wallet['blockchain']:
                ethereum_wallets.append(wallet)

        return [
            BitcoinToken(wallets=bitcoin_wallets),
            EthereumToken(wallets=ethereum_wallets)]
