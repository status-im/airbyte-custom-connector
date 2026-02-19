import requests, logging, re, datetime, time
from urllib.parse import urlparse, parse_qsl
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode

class EtherscanStream(HttpStream):
    pagination_offset = 150
    url_base = "https://api.etherscan.io/"
    WEI_DECIMALS = 18
    GWEI_DECIMALS = 9
    EMPTY_ADDRESS = "0x0000000000000000000000000000000000000000"

    primary_key = None
    cursor_field = []

    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.wallets = wallets
        self.chain_id = chain_id
        self.wallet_info = {
            wallet["address"]: {
                "tags": wallet["tags"],
                "name": wallet["name"]
            }
            for wallet in self.wallets
        }
        self.is_balance_stream = self.name.endswith('balance')
        self.sleep_seconds = sleep_seconds
        self.logger.info(f"{self.name} > Sleep per request: {self.sleep_seconds}s")

        yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
        # Syncing since Ethereum first transaction
        start_date = yesterday if not backfill else datetime.date(year=2015, month=7, day=30)

        self.historical_mapping = {
            wallet["address"]: {
                "start_date": start_date,
                "end_date": yesterday,
            }
            for wallet in self.wallets
        }

    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:

        for wallet in self.wallets:
            selected = self.historical_mapping[wallet["address"]]
            msg = f"{self.name} > stream_slice: Fetching data for {wallet['name']}" + ("" if self.is_balance_stream else f" from {selected['start_date']} to {selected['end_date']}")
            self.logger.info(msg)
            time.sleep(self.sleep_seconds)
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
        if not result:
            return None

        seconds = 0
        if "rate limit reached" in str(result).lower():
            match = re.search(r"\((\d+)\s*/", str(result))
            seconds = int(match.group(1))

        elif self.is_balance_stream:
            seconds = 1

        self.logger.info(f"{self.name} > backoff_time: {seconds}s")
        return seconds

    def next_page_token(self, response: requests.Response):

        if self.is_balance_stream:
            return None

        result = response.json().get("result", [])
        if isinstance(result, str):
            seconds = self.backoff_time(response)
            time.sleep(seconds)
            # Retry same page
            current_page = int(self.get_params(response).get("page", 1))
            return {"page": current_page}

        if not result:
            return None

        params = self.get_params(response)
        current_page = int(params.get("page", 1))
        # Last page may have less records
        if len(result) < self.pagination_offset:
            return None

        to_lower = lambda result: {key.lower(): value for key, value in result.items()}
        earliest_date = self.to_datetime(to_lower(result[-1])["timestamp"])
        params = {"page": current_page + 1} if self.is_valid(params["address"], earliest_date) else None
        time.sleep(self.sleep_seconds)
        return params

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        if not stream_slice:
            return {}

        params = {
            "chainid": self.chain_id,
            "address": stream_slice["address"],
            "apikey": self.api_key,
            "sort": "desc",
            "module": "account",
            "offset": self.pagination_offset,
            "page": next_page_token["page"] if next_page_token else 1
        }
        if self.is_balance_stream:
            params.pop("sort")
            params.pop("offset")
            params.pop("page")

        self.logger.info(f"{self.name} > request_params: {params}")
        return params

    def to_datetime(self, timestamp: str) -> datetime.datetime:
        """
        Convert the default Etherscan timestamp to a `datetime`
        """
        return datetime.datetime.fromtimestamp(int(timestamp))

    def is_valid(self, wallet_address: str, timestamp: datetime.datetime) -> bool:
        """
        Check if the transaction is between the specified wallet's start and end date
        """
        start_date = self.historical_mapping[wallet_address]["start_date"]
        end_date = self.historical_mapping[wallet_address]["end_date"]
        return timestamp.date() <= end_date and timestamp.date() >= start_date

    def has_finished(self, wallet_address: str, timestamp: datetime.datetime) -> bool:
        """
        Check if the given timestamp is before the start date.
        If `"sort": "desc"` is modified then `start_date` should be replaced with `end_date`
        """
        start_date = self.historical_mapping[wallet_address]["start_date"]
        return timestamp.date() < start_date

    def get_params(self, response: requests.Response) -> dict:
        """
        Get the parameters used for the GET request
        """
        parsed_url = urlparse(response.request.path_url)
        return dict(parse_qsl(parsed_url.query))

    def camel_to_title(self, text: str) -> str:
        """
        Convert transaction Method information into etherscan.io format
        """
        if len(text) == 0:
            return None

        name = text.split("(", 1)[0]
        name = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
        name = name.replace("_", " ")
        return name.title()

    def get_transactions(self, response: requests.Response) -> list:
        """
        Get the transactions from the current request.
        Avoids `TypeError: 'NoneType' object is not iterable`
        """
        data: dict = response.json()
        txs: list[dict] = data.get("result", [])
        txs = txs if txs else []
        return txs

class WalletTransactions(EtherscanStream):
    """
    A transaction where an EOA (Externally Owned Address, or typically referred to as a wallet address) sends ETH directly to another EOA.
    When viewing an address on Etherscan, this type of transaction will be shown under the Transaction tab.
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "txlist",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        txs = self.get_transactions(response)
        params = self.get_params(response)
        selected = self.wallet_info.get(params["address"], {})

        for trx in txs:
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(params["address"], timestamp):
                break

            if not self.is_valid(params["address"], timestamp):
                continue

            method_call = trx["functionName"] if len(trx["functionName"]) > 0 else None

            point = {
                "wallet_address": params["address"],
                "wallet_name": selected["name"],
                "tags": selected["tags"],
                "hash": trx["hash"],
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": trx["from"],
                "movement": None,
                "to_address": trx["to"],
                "amount": trx["value"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.WEI_DECIMALS,
                "chain_id": int(self.chain_id),
                "gas_price": trx["gasPrice"],
                "gas_used": trx["gasUsed"],
                "gas_decimals": self.WEI_DECIMALS,
                "method_id": trx["methodId"],
                "method_call": method_call,
                "method_name": self.camel_to_title(method_call) if method_call else trx["methodId"],
                "is_error": bool(int(trx["isError"]))
            }
            if len(point["to_address"]) == 0:
                point["to_address"] = point["wallet_address"]

            if point["from_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "out"
            elif point["to_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "in"

            yield point

class WalletInternalTransactions(EtherscanStream):
    """
    This refers to a transfer of ETH that is carried out through a smart contract as an intermediary.
    When viewing an address on Etherscan, this type of transaction will be shown under the Internal Txns tab
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "txlistinternal",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        txs = self.get_transactions(response)
        params = self.get_params(response)
        selected = self.wallet_info.get(params["address"], {})

        for trx in txs:
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(params["address"], timestamp):
                break

            if not self.is_valid(params["address"], timestamp):
                continue

            point = {
                "wallet_address": params["address"],
                "wallet_name": selected["name"],
                "tags": selected["tags"],
                "hash": trx["hash"],
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": trx["from"],
                "movement": None,
                "to_address": trx["to"],
                "amount": trx["value"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.WEI_DECIMALS,
                "chain_id": int(self.chain_id),
                "gas": trx["gas"],
                "gas_used": trx["gasUsed"],
                "gas_decimals": self.WEI_DECIMALS,
                "is_error": bool(int(trx["isError"]))
            }
            if len(point["to_address"]) == 0:
                point["to_address"] = point["wallet_address"]

            if point["from_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "out"
            elif point["to_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "in"

            yield point

class WalletTokenTransactions(EtherscanStream):
    """
    Transactions of ERC-20 or ERC-721 tokens are labelled as Token Transfer transactions.
    When viewing an address on Etherscan, this type of transaction will be shown under either the Erc20 Token Txns or Erc721 Token Txns tab, depending on the respective token type.
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "tokentx",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        txs = self.get_transactions(response)
        params = self.get_params(response)
        selected = self.wallet_info.get(params["address"], {})

        for trx in txs:
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(params["address"], timestamp):
                break

            if not self.is_valid(params["address"], timestamp):
                continue

            method_call = trx["functionName"] if len(trx["functionName"]) > 0 else None

            point = {
                "wallet_address": params["address"],
                "wallet_name": selected.get("name"),
                "tags": selected.get("tags"),
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
                "gas_decimals": self.WEI_DECIMALS,
                "chain_id": int(self.chain_id),
                "is_error": False
            }
            if len(point["to_address"]) == 0:
                point["to_address"] = point["wallet_address"]

            if point["from_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "out"
            elif point["to_address"].lower() == point["wallet_address"].lower():
                point["movement"] = "in"

            yield point

class NativeBalance(EtherscanStream):
    """
    Native balance (ETH) for the wallets

    NOTE: Used for debugging purposes
    """

    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def next_page_token(self, response: requests.Response):
        time.sleep(self.sleep_seconds)
        return None

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "balance",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        data: dict = response.json()
        params = self.get_params(response)
        wallet_address = params["address"]
        wallet = self.wallet_info[wallet_address]
        point = {
            "timestamp": datetime.datetime.now(),
            "wallet_address": wallet_address,
            "wallet_name": wallet["name"],
            "tags": wallet["tags"],
            "token_symbol": "ETH",
            "token_decimal": self.WEI_DECIMALS,
            "amount": data["result"],
            "chain_id": int(self.chain_id)
        }
        yield point


class MinedBlocks(EtherscanStream):
    """
    Get newly minted ETH block rewards earned by the wallet
    """
    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "getminedblocks",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        txs = self.get_transactions(response)
        params = self.get_params(response)
        selected = self.wallet_info.get(params["address"], {})

        for trx in txs:
            timestamp = self.to_datetime(trx["timeStamp"])
            if self.has_finished(params["address"], timestamp):
                break

            if not self.is_valid(params["address"], timestamp):
                continue

            point = {
                "wallet_address": params["address"],
                "wallet_name": selected.get("name"),
                "tags": selected.get("tags"),
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": self.EMPTY_ADDRESS,
                "movement": "in",
                "to_address": params["address"],
                "amount": trx["blockReward"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.WEI_DECIMALS,
                "chain_id": int(self.chain_id),
                "is_error": False
            }

            yield point

class BeaconWithdrawals(EtherscanStream):

    def __init__(self, api_key: str, wallets: list[dict], chain_id: str, backfill: bool, sleep_seconds: int, **kwargs):
        super().__init__(api_key, wallets, chain_id, backfill, sleep_seconds, **kwargs)

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            **super().request_params(stream_state, stream_slice, next_page_token),
            "action": "txsBeaconWithdrawal",
        }
        return params

    def parse_response(self, response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        txs = self.get_transactions(response)
        params = self.get_params(response)
        selected = self.wallet_info.get(params["address"], {})

        for trx in txs:
            timestamp = self.to_datetime(trx["timestamp"])
            if self.has_finished(params["address"], timestamp):
                break

            if not self.is_valid(params["address"], timestamp):
                continue

            point = {
                "wallet_address": params["address"],
                "wallet_name": selected.get("name"),
                "tags": selected.get("tags"),
                "block": int(trx["blockNumber"]),
                "timestamp": timestamp,
                "from_address": self.EMPTY_ADDRESS,
                "movement": "in",
                "to_address": params["address"],
                "amount": trx["amount"],
                "token_name": "Ethereum",
                "token_symbol": "ETH",
                "token_decimal": self.GWEI_DECIMALS,
                "chain_id": int(self.chain_id),
                "is_error": False,
                "withdrawal_index": trx["withdrawalIndex"],
                "validator_index": trx["validatorIndex"],
            }

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
            logger.info(f"Params: {params}")
            params["apikey"] = config["api_key"]
            response = requests.get(self.url, params=params)
            logger.info(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                continue

            failed.append(f"Failed connection check for {wallet}")

        return len(failed) == 0, "\n".join(failed) if failed else None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        params = {
            **config,
            "wallets": [
                {
                    "tags": wallet["tags"],
                    "name": wallet["name"],
                    # Etherscan addresses are always lowercase
                    "address": wallet["address"]
                }
                for wallet in config["wallets"]
            ]
        }
        streams = [
            WalletTransactions(**params),
            WalletInternalTransactions(**params),
            WalletTokenTransactions(**params),
            NativeBalance(**params),
            MinedBlocks(**params),
            BeaconWithdrawals(**params)
        ]
        return streams
