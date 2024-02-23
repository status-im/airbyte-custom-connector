from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams.http import HttpStream
import logging
import requests
import json
import time
from .utils import extract_token
logger = logging.getLogger("airbyte")


class BlockchainStream(HttpStream):
   
   primary_key = None


   def __init__(self, wallets:  List['str'], **kwargs):
        super().__init__(**kwargs)
        self.wallets = wallets

   def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            yield {
                "address":  wallet['address'],
                "name":     wallet['name'],
                "tags":     wallet['tags']
            }

   def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

   def backoff_time(self, response: requests.Response) -> Optional[float]:
        """This method is called if we run into the rate limit.
        Slack puts the retry time in the `Retry-After` response header so we
        we return that value. If the response is anything other than a 429 (e.g: 5XX)
        fall back on default retry behavior.
        """
        if "Retry-After" in response.headers:
            return int(response.headers["Retry-After"])
        else:
            self.logger.info("Retry-after header not found. Using default backoff value")
            return 5



class BitcoinToken(BlockchainStream):

    url_base = "https://blockchain.info/rawaddr/"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Getting Bitcoin Balance information")
        logger.info("Response %s",  response.json())
        bitcoin_data = response.json()
        yield {
             "wallet_name": stream_slice['name'],
             "name":"BTC", 
             "symbol":"BTC", 
             "description": "Bitcoin", 
             "chain": "bitcoin",
             "balance": bitcoin_data['final_balance'],
             "decimal":8,
             "tags": stream_slice['tags']
        }

class EthereumToken(BlockchainStream):

    url_base = "https://api.ethplorer.io/getAddressInfo/"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}?apiKey=freekey"

    def parse_response(self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Getting ETH balance information %s", stream_slice['name'])
        eth_data=response.json()['ETH']
        yield {
            "wallet_name": stream_slice['name'],
            "name":"ETH", 
            "symbol":"ETH", 
            "description": "Native Ethereum token", 
            "chain": "Ethereum",
            "balance":eth_data['rawBalance'],
            "decimal":18,
            "tags": stream_slice['tags'],
            "address": "eth"
        }
        if 'tokens' in response.json():
            tokens_data=response.json()['tokens']
            for t in tokens_data:
                try:
                    yield extract_token(stream_slice['name'], t)
                except Exception as e:
                    name = t 
                    if 'name' in t:
                        name= t['name']
                    logger.warning('Dropping token %s not valid %s',name, e )
        # Delaying calls - Not great but that works
        time.sleep(2)
