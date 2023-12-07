from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams.http import HttpStream
import logging
import requests
import json


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
                "name":     wallet['name']
            }

   def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None




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

class EthereumToken(BlockchainStream):

    url_base = "https://api.ethplorer.io/getAddressInfo/"

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


