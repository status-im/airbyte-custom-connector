#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests, logging, time, re, random

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import NoAuth
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType
from airbyte_cdk.sources.streams.http.exceptions import UserDefinedBackoffException


from datetime import datetime, timedelta
from .models import Wallet, Token

logger = logging.getLogger("airbyte")


def get_block_by_timestamp(api_key: str, chain_id: str, timestamp: str) -> str:
    res = requests.get(f"https://api.etherscan.io/v2/api?chainid={chain_id}&module=block&action=getblocknobytime&timestamp={int(timestamp)}&closest=before&apikey={api_key}")
    res.raise_for_status()
    logger.info("Block to check: %s", res.json().get('result'))
    return res.json().get('result')

# Basic full refresh stream
class EtherscanStream(HttpStream, ABC):
    url_base = "https://api.etherscan.io/"

    def __init__(self, chain_id: str, api_key: str, wallets: List[Wallet], startblock: str, **kwargs):
        super().__init__(**kwargs)
        self.chain_id = chain_id
        self.api_key = api_key
        self.wallets = wallets
        self.startblock = startblock

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for w in self.wallets:
            yield {"name": w.name, "address": w.address, "tag": w.tag}

    def path(self, **kwargs) -> str:
        return "v2/api"
    
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        output: dict = response.json()
        result = output.get("result")
        
        seconds = 0
        if "rate limit reached" in str(result).lower():
            match = re.search(r"\((\d+)\s*/", str(result))
            seconds = int(match.group(1))
        logger.info(f"backoff_time: {seconds}s")
        return seconds
    
    def raise_for_rate_limit(self, response: requests.Response, stream_slice: Mapping[str, any]):

        output: dict = response.json()
        result = output.get("result")

        name = stream_slice["name"]
        address = stream_slice["address"]
        logger.info(f"{name} - {address}")

        if "rate limit reached" in str(result).lower():
            logger.info("TRIGGERING CUSTOM EXCEPTION")
            match = re.search(r"(\d+)\s*", str(result))
            multiplier = random.randint(1, 10)
            backoff = (int(match.group(0)) if match else 1) * multiplier
            raise UserDefinedBackoffException(backoff, response.request, response, result)
        
        time.sleep(0.55)

class InternalBalance(EtherscanStream):
    primary_key = "wallet_address"
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "chainid": self.chain_id,
            "module":"account",
            "action":"balance",
            "address":stream_slice["address"],
            "tag": "latest",
            "apikey": self.api_key
        }
        logger.debug(params)
        return params

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, any] = None, **kwargs) -> Iterable[Mapping]:
        logger.debug("stream_slice : %s", stream_slice )
        balance = response.json()
        self.raise_for_rate_limit(response, stream_slice)
        yield {
            "wallet_name": stream_slice["name"],
            "wallet_address": stream_slice["address"],
            "tag": stream_slice["tag"],
            "balance": balance.get("result"),
            "chain": self.chain_id
        }

class TokenBalance(EtherscanStream):
    primary_key = "wallet_address"

    def __init__(self, tokens: List[Token],**kwargs):
        super().__init__(**kwargs)
        self.tokens = tokens

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for w in self.wallets:
            for t in self.tokens:
                logger.debug("token name: %s, token addr %s", t.name, t.address)
                yield {"name": w.name, "address": w.address, "tag": w.tag, "token": t.name, "tokenAddress": t.address}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        logger.debug("stream slice: %s", stream_slice)
        params = {
            "chainid": self.chain_id,
            "module":"account",
            "action":"tokenbalance",
            "address":stream_slice["address"],
            "contractaddress": stream_slice["tokenAddress"],
            "apikey": self.api_key
        }
        logger.debug(params)
        return params

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, any] = None, **kwargs) -> Iterable[Mapping]:
        balance = response.json()
        self.raise_for_rate_limit(response, stream_slice)
        logger.debug("Balance : %s", balance)
        yield {
            "wallet_name": stream_slice["name"],
            "wallet_address": stream_slice["address"],
            "tag": stream_slice["tag"],
            "token": stream_slice["token"],
            "tokenAddress": stream_slice["tokenAddress"],
            "balance": balance.get("result"),
            "chain": self.chain_id
        }




"""
Fetching Internal transactions
"""

class InternalTransaction(EtherscanStream):
    primary_key = "hash"
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "chainid": self.chain_id,
            "module":"account",
            "action":"txlistinternal",
            "address": stream_slice["address"],
            "startblock": self.startblock,
            "apikey": self.api_key,
            "sort": "asc"
        }
        logger.debug(params)
        return params


    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, any] = None, **kwargs) -> Iterable[Mapping]:
        res = response.json()
        self.raise_for_rate_limit(response, stream_slice)
        logger.debug("response: %s", res)
        for trx in res.get("result"):
            yield {
                "wallet_name": stream_slice["name"],
                "wallet_address": stream_slice["address"],
                "tag": stream_slice["tag"],
                "chain": self.chain_id,
                "value": trx.get("value"),
                "to": trx.get("to"),
                "from": trx.get("from"),
                "gas": trx.get("gas"),
                "hash": trx.get("hash"),
                "blockNumber": trx.get("blockNumber"),
                "timestamp": trx.get("timeStamp")
            }


"""
Strean to fetch the list of ERC20 transaction to a wallet
"""
class TokenTransaction(EtherscanStream):
    primary_key = "wallet_address"
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "chainid": self.chain_id,
            "module":"account",
            "action":"tokentx",
            "address":stream_slice["address"],
            "startblock": self.startblock,
            "sort": "asc",
            "apikey": self.api_key
        }
        logger.debug(params)
        return params

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, any] = None, **kwargs) -> Iterable[Mapping]:
        res = response.json()
        self.raise_for_rate_limit(response, stream_slice)
        for trx in res.get("result"):
            yield {
                "wallet_name": stream_slice["name"],
                "wallet_address": stream_slice["address"],
                "tag": stream_slice["tag"],
                "chain": self.chain_id,
                "value": trx.get("value"),
                "to": trx.get("to"),
                "from": trx.get("from"),
                "contractAddress": trx.get("contractAddress"),
                "tokenName": trx.get("tokenName"),
                "tokenSymbol": trx.get("tokenSymbol"),
                "tokenDecimal": trx.get("tokenDecimal"),
                "gas": trx.get("gas"),
                "gasPrice": trx.get("gasPrice"),
                "gasUsed": trx.get("gasUsed"),
                "nonce": trx.get("nonce"),
                "hash": trx.get("hash"),
                "blockHash": trx.get("blockHash"),
                "blockNumber": trx.get("blockNumber"),
                "timestamp": trx.get("timeStamp")
            }

# Source
class SourceEtherscan(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        get_block_by_timestamp(config["api_key"], config['chain_id'], (datetime.today() - timedelta(days=config.get("delta"))).timestamp())
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        default_args = {
            "api_key": config["api_key"],
            "chain_id": config["chain_id"],
            "wallets": [Wallet(name=w.get("name"), address=w.get("address"), tag=w.get("tag")) for w in config["wallets"]],
            "startblock": get_block_by_timestamp(config["api_key"], config['chain_id'], (datetime.today() - timedelta(days=config.get("delta"))).timestamp()),
        }

        streams = [
            InternalBalance(**default_args),
            InternalTransaction(**default_args),
            TokenTransaction(**default_args)
        ]
        if 'tokens' in config:
            streams.append(TokenBalance(tokens=[Token(name=t.get("name"),address=t.get("address")) for t in config["tokens"]],**default_args))
        logger.info(f"Streams: {streams}")
        return streams

