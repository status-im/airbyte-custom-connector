from typing import Any, Iterable, List, Mapping, Optional
from airbyte_cdk.sources.streams.http import HttpStream
import requests
import time
from .utils import extract_token


class BlockchainStream(HttpStream):
    primary_key = None

    def __init__(self, wallets: List[str], api_key: str = 'freekey', **kwargs):
        super().__init__(**kwargs)
        self.wallets = wallets
        self.api_key = api_key

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            yield {
                "address": wallet['address'],
                "name": wallet['name'],
                "tags": wallet['tags']
            }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """Handle rate limiting with appropriate backoff"""
        if response.status_code == 429:
            return int(response.headers.get("Retry-After", 60))
        elif "Retry-After" in response.headers:
            return int(response.headers["Retry-After"])
        else:
            return 10


class BitcoinToken(BlockchainStream):
    name = "bitcoin_token"
    url_base = "https://blockchain.info/rawaddr/"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        bitcoin_data = response.json()
        
        # Convert satoshis to BTC and format as string
        raw_balance = bitcoin_data.get('final_balance', 0)
        try:
            balance_btc = float(raw_balance) / (10 ** 8)
            balance_str = str(balance_btc)
        except (ValueError, OverflowError):
            balance_str = "0"
            
        yield {
            "wallet_name": stream_slice['name'],
            "name": "BTC",
            "symbol": "BTC",
            "description": "Bitcoin",
            "chain": "bitcoin",
            "balance": balance_str,
            "decimal": "8",
            "tags": stream_slice['tags']
        }


class EthereumToken(BlockchainStream):
    name = "ethereum_token"
    url_base = "https://api.ethplorer.io/getAddressInfo/"

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return f"{stream_slice['address']}?apiKey={self.api_key}"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        wallet_name = stream_slice.get('name', 'Unknown')
        
        try:
            response_data = response.json()
            
            # Process ETH balance
            if 'ETH' in response_data:
                eth_data = response_data['ETH']
                raw_balance = eth_data.get('rawBalance', 0)
                
                try:
                    balance_eth = float(raw_balance) / (10 ** 18)
                    balance_str = str(balance_eth)
                except (ValueError, OverflowError):
                    balance_str = "0"
                
                yield {
                    "wallet_name": wallet_name,
                    "name": "ETH",
                    "symbol": "ETH",
                    "description": "Native Ethereum token",
                    "chain": "Ethereum",
                    "balance": balance_str,
                    "decimal": "18",
                    "tags": stream_slice['tags'],
                    "address": "eth"
                }
            
            # Process ERC-20 tokens
            if 'tokens' in response_data:
                for token_data in response_data['tokens']:
                    try:
                        token = extract_token(wallet_name, token_data)
                        token['tags'] = stream_slice['tags']
                        yield token
                    except Exception:
                        # Skip invalid tokens
                        continue
                        
        except Exception:
            # Skip wallets with invalid responses
            pass
            
        # Rate limiting delay
        time.sleep(2)