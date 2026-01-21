import requests, logging, re, datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

class EtherscanStream(HttpStream):
    primary_key = "hash"
    pagination_offset = 50
    url_base = "https://api.etherscan.io/"
    ETHEREUM_DECIMALS = 18

    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.wallets = wallets
        self.chain_id = chain_id
        
        self.historical_mapping = {
            wallet["address"]: {
                "start_date": self.get_ref_date(wallet.get("start_date")),
                "end_date": self.get_ref_date(wallet.get("end_date")),
            }
            for wallet in self.wallets
        }

        self.page_counter = {
            wallet["address"]: 1
            for wallet in self.wallets
        }

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            selected = self.historical_mapping[wallet["address"]]
            self.logger.info(f"{self.name} > stream_slice: Fetching data for {wallet['name']} from {selected['start_date']} to {selected['end_date']}")
            yield {
                "address": wallet["address"],
                "name": wallet["name"],
                "tags": wallet["tags"],
            }
    
    def path(self, **kwargs) -> str:
        return "v2/api"
    
    def backoff_time(self, response: requests.Response) -> Optional[float]:
        output: dict = response.json()
        result = output.get("result")
        
        seconds = 0
        if "rate limit reached" in str(result).lower():
            match = re.search(r"\((\d+)\s*/", str(result))
            seconds = int(match.group(1))
        
        self.logger.info(f"{self.name} > backoff_time: {seconds}s")
        return seconds

    def next_page_token(self, response: requests.Response):
        def find_address(wallets: list[str], trx: dict) -> Optional[str]:
            if trx["to"] in wallets:
                return trx["to"]
            if trx["from"] in wallets:
                return trx["from"]
            
            return None
        
        result: list[dict] = response.json().get("result", [])
        if not result or not isinstance(result, list):
            return None
        
        wallet_address = find_address(list(self.page_counter.keys()), result[0])
        if not wallet_address:
            return None
        
        self.page_counter[wallet_address] += 1
        params = {
            "page": self.page_counter[wallet_address]
        }

        return params

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            "chainid": self.chain_id,
            "address": stream_slice["address"],
            "apikey": self.api_key,
            "sort": "desc",
            "module": "account",
            "offset": self.pagination_offset
        }
        if next_page_token:
            params["page"] = self.page_counter[stream_slice["address"]]
        
        self.logger.info(f"{self.name} > request_params: {params}")
        return params

    def to_datetime(self, timestamp: str) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(timestamp))

    def is_valid(self, wallet_address: str, timestamp: datetime.datetime) -> bool:
        start_date = self.historical_mapping[wallet_address]["start_date"]
        end_date = self.historical_mapping[wallet_address]["end_date"]
        return timestamp.date() <= end_date and timestamp.date() >= start_date
    
    def has_finished(self, wallet_address: str, timestamp: datetime.datetime) -> bool:
        start_date = self.historical_mapping[wallet_address]["start_date"]
        return timestamp.date() < start_date
    
    @staticmethod
    def get_ref_date(date: Optional[str]) -> datetime.date:
        """
        Convert the Airbyte config dates to dates must be in YYYY-MM-DD format
        """
        previous_day = datetime.datetime.now().date() - datetime.timedelta(days=1)
        if not date or (isinstance(date, str) and len(date) == 0):
            return previous_day
        
        return datetime.datetime.strptime(date, "%Y-%m-%d").date()


class WalletTransactions(EtherscanStream):
    """
    A transaction where an EOA (Externally Owned Address, or typically referred to as a wallet address) sends ETH directly to another EOA. 
    When viewing an address on Etherscan, this type of transaction will be shown under the Transaction tab. 
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, **kwargs):
        super().__init__(api_key, wallets, chain_id, **kwargs)
    
    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "txlist",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        
        data: dict = response.json()
        txs: list[dict] = data.get("result", [])
        
        for trx in txs:
            if not isinstance(trx, dict):
                continue
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(stream_slice["address"], timestamp):
                break
            
            if not self.is_valid(stream_slice["address"], timestamp):
                continue
            
            point = {
                "wallet_address": stream_slice["address"],
                "wallet_name": stream_slice["name"],
                "tags": stream_slice["tags"],
                "hash": trx["hash"],
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": trx["from"],
                "movement": None,
                "to_address": trx["to"],
                "amount": trx["value"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.ETHEREUM_DECIMALS,
                "chain_id": int(self.chain_id),
                "gas_price": trx["gasPrice"],
                "gas_used": trx["gasUsed"],
                "gas_decimals": self.ETHEREUM_DECIMALS
            }
            if trx["from"] == stream_slice["address"]:
                point["movement"] = "out"
            elif trx["to"] == stream_slice["address"]:
                point["movement"] = "in"
            
            yield point


class WalletInternalTransactions(EtherscanStream):
    """
    This refers to a transfer of ETH that is carried out through a smart contract as an intermediary. 
    When viewing an address on Etherscan, this type of transaction will be shown under the Internal Txns tab
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, **kwargs):
        super().__init__(api_key, wallets, chain_id, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "txlistinternal",
        }
        return params
    
    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        
        data: dict = response.json()
        txs: list[dict] = data.get("result", [])
        
        for trx in txs:
            if not isinstance(trx, dict):
                continue
            timestamp = self.to_datetime(trx["timeStamp"])            
            if self.has_finished(stream_slice["address"], timestamp):
                break
            
            if not self.is_valid(stream_slice["address"], timestamp):
                continue

            point = {
                "wallet_address": stream_slice["address"],
                "wallet_name": stream_slice["name"],
                "tags": stream_slice["tags"],
                "hash": trx["hash"],
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": trx["from"],
                "movement": None,
                "to_address": trx["to"],
                "amount": trx["value"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.ETHEREUM_DECIMALS,
                "chain_id": int(self.chain_id),
            }
            if trx["from"] == stream_slice["address"]:
                point["movement"] = "out"
            elif trx["to"] == stream_slice["address"]:
                point["movement"] = "in"

            yield point


class WalletTokenTransactions(EtherscanStream):
    """
    Transactions of ERC-20 or ERC-721 tokens are labelled as Token Transfer transactions. 
    When viewing an address on Etherscan, this type of transaction will be shown under either the Erc20 Token Txns or Erc721 Token Txns tab, depending on the respective token type.
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, **kwargs):
        super().__init__(api_key, wallets, chain_id, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "tokentx",
        }
        return params

    def camel_to_title(self, text: str) -> str:
        if len(text) == 0:
            return None

        name = text.split("(", 1)[0]
        name = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
        name = name.replace("_", " ")
        return name.title()
    
    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        
        data: dict = response.json()
        txs: list[dict] = data.get("result", [])

        for trx in txs:
            if not isinstance(trx, dict):
                continue
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(stream_slice["address"], timestamp):
                break
            
            if not self.is_valid(stream_slice["address"], timestamp):
                continue

            method_call = trx["functionName"] if len(trx["functionName"]) > 0 else None
            point = {
                "wallet_address": stream_slice["address"],
                "wallet_name": stream_slice["name"],
                "tags": stream_slice["tags"],
                "hash": trx["hash"],
                "method_id": trx["methodId"],
                "method_call": method_call,
                "method_name": self.camel_to_title(method_call) if method_call else trx["methodId"],
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": trx["from"],
                "movement": None,
                "to_address": trx["to"],
                "amount": trx["value"],
                "token_name": trx["tokenName"],
                "token_symbol": trx["tokenSymbol"],
                "token_decimal": int(trx["tokenDecimal"]),
                "token_address": trx["contractAddress"],
                "gas_price": trx["gasPrice"],
                "gas_used": trx["gasUsed"],
                "gas_decimals": self.ETHEREUM_DECIMALS,
                "chain_id": int(self.chain_id),
            }
            if trx["from"] == stream_slice["address"]:
                point["movement"] = "out"
            elif trx["to"] == stream_slice["address"]:
                point["movement"] = "in"

            yield point

class SourceEtherscan(AbstractSource):
    
    url = "https://api.etherscan.io/v2/api"

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        
        logger.info(f"URL: {self.url}")
        failed = []
        wallets: list[dict] = config["wallets"]
        for wallet in wallets:
            params = {
                "chainid": config["chain_id"],
                "module": "account",
                "action": "balance",
                "address": wallet["address"]
            }
            # Just to verify that the dates are in YYYY-MM-DD format
            start_date = EtherscanStream.get_ref_date(wallet.get("start_date"))
            end_date = EtherscanStream.get_ref_date(wallet.get("end_date"))
            if start_date > end_date:
                failed.append(f"Wallet {wallet['address']} has a start date ({start_date}) greater than its end date ({end_date})")
                continue

            logger.info(f"Params: {params}")
            params["apikey"] = config["api_key"]
            response = requests.get(self.url, params=params)
            logger.info(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                continue
            
            failed.append(f"Failed connection check for {wallet}")

        return len(failed) == 0, "\n".join(failed) if failed else None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = [
            WalletTransactions(**config),
            WalletInternalTransactions(**config),
            WalletTokenTransactions(**config),
        ]
        return streams

