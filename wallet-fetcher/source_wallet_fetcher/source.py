#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


#from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from .utils import extract_token
from .stream import BitcoinToken, EthereumToken
import logging
import requests
import json
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")
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
