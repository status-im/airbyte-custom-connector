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

# Basic full refresh stream
class Token(HttpStream):
    # TODO: Fill in the url base. Required.
    url_base = "https://api.ethplorer.io/getAddressInfo/"

    # Set this as a noop.
    primary_key = None

    def __init__(self, wallet_address: str, wallet_name: str, **kwargs):
        super().__init__(**kwargs)
        self.wallet_address = wallet_address
        self.wallet_name = wallet_name

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(self, **kwargs) -> str:
        address = self.wallet_address
        return f"{address}?apiKey=freekey"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"wallet_address": self.wallet_address}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.info("Getting ETH balance information")
        eth_data=response.json()['ETH']
        yield {
               "name":"ETH", 
               "symbol":"ETH", 
               "description": "Native Ethereum token", 
               "address":"", 
               "chain": "Ethereum",
               "balance":eth_data['rawBalance']
               , "decimal":18
               }
        logging.info("Fetching Tokens balance information")
        tokens_data=response.json()['tokens']
        for t in tokens_data:
            try:
                yield extract_token(self.wallet_name, t)
            except Exception as e:
                logger.error('Dropping token not valid %s' % t )
# Source
class SourceWalletFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        # TODO add a check for each endpoint
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        tokens: List[Token] = []

        for wallet in config["wallets"]:
            tokens.append(
                Token(
                    wallet_address=wallet['address'], 
                    wallet_name=wallet['name']
                )
            )
        return tokens
