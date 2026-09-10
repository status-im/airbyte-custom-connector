import requests, logging, json, datetime, re
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode

class LogosExecutionZoneStream(HttpStream):

    http_method = "POST"
    cursor_field = "block_id"
    primary_key = "hash"

    def __init__(self, url_base: str, rpc_method: str, latest_block_id: int):
        super().__init__()
        self.logger.info(f"{self.name} > Uses RPC method {rpc_method} [{url_base}]")
        self.__latest_block_id = latest_block_id
        self.__url_base = url_base
        self.__rpc_method = rpc_method

    @property
    def url_base(self):
        return self.__url_base

    def next_page_token(self, response: requests.Response):
        return None

    def path(self, **kwargs) -> str:
        return ""

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"block_id": latest_record["block_id"]}

    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_block = stream_state.get("block_id", 1) if stream_state else 1
        self.logger.info(f"Start block: {start_block}")
        self.logger.info(f"Latest block: {self.__latest_block_id}")
        for id, block_id in enumerate(range(start_block, self.__latest_block_id + 1)):
            self.logger.info(f"{self.name} > Starting {block_id} / {self.__latest_block_id}")
            yield {
                "block_id": block_id,
                "rpc_method": self.__rpc_method,
                "rpc_call_id": id + 1
                # NOTE: `params` must be done in Child Streams
            }

    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None):
        data = {
            "jsonrpc": "2.0",
            "id": stream_slice["rpc_call_id"],
            "method": stream_slice["rpc_method"],
        }
        return data

    def camel_to_snake(self, name: str) -> str:
        """
        Convert a camelCase or PascalCase string to snake_case.

        Parameters:
            - `name` - the camel case value

        Output:
            - snake case value
        """
        s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
        return s.lower()

class BlockInfo(LogosExecutionZoneStream):

    TRANSACTION_TYPES = ["Public", "PrivacyPreserving", "ProgramDeployment"]

    def __init__(self, url_base: str, rpc_method: str, latest_block_id: int, cache: MutableMapping[int, Any] = None):
        super().__init__(url_base, rpc_method, latest_block_id)
        # Shared with LEZTransactions so a block is fetched from LEZ only once.
        self._cache = cache if cache is not None else {}

    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None):
        data = super().request_body_json(stream_state, stream_slice, next_page_token)
        data["params"] = [stream_slice.get("block_id", 1)]
        return data

    def extract_block(self, response: requests.Response, stream_slice: Mapping[str, Any]) -> Tuple[dict, list]:
        data = response.json()["result"]
        transactions: list[dict] = data["body"]["transactions"]
        transaction_info = {}
        for current_type in self.TRANSACTION_TYPES:
            transaction_info[f"{self.camel_to_snake(current_type)}_transactions"] = sum([current_type in transaction.keys() for transaction in transactions])

        transaction_info["total_transactions"] = sum(transaction_info.values())
        block = {
            "block_id": data["header"]["block_id"],
            "hash": data["header"]["hash"],
            "prev_block_hash": data["header"]["prev_block_hash"],
            "timestamp": datetime.datetime.fromtimestamp(data["header"]["timestamp"] / 1_000),
            "timezone": "UTC",
            **transaction_info,
            "status": data["bedrock_status"],
            "rpc_method": stream_slice["rpc_method"]
        }
        return block, transactions

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        block, transactions = self.extract_block(response, stream_slice)
        self._cache[block["block_id"]] = (block, transactions)
        yield block

class BlockSubStream(BlockInfo):
    """
    Base for any stream whose records are derived from a LEZ block.
    """

    def __init__(self, url_base: str, rpc_method: str, latest_block_id: int, cache: MutableMapping[int, Any]):
        super().__init__(url_base, rpc_method, latest_block_id, cache=cache)

    def records_from_block(self, block: Mapping[str, Any], transactions: list) -> Iterable[Mapping[str, Any]]:
        raise NotImplementedError

    def read_records(self, sync_mode: SyncMode, cursor_field: List[str] = None, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:
        cached = self._cache.get(stream_slice["block_id"])
        if cached:
            yield from self.records_from_block(*cached)
        else:
            yield from super().read_records(sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state)

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        block, transactions = self.extract_block(response, stream_slice)
        self._cache.setdefault(block["block_id"], (block, transactions))
        yield from self.records_from_block(block, transactions)


class PublicTransactions(BlockSubStream):

    primary_key = "hash"

    def records_from_block(self, block: Mapping[str, Any], transactions: list) -> Iterable[Mapping[str, Any]]:
        for current in transactions:
            transaction: dict = current.get("Public") or {}
            if not transaction:
                continue

            program_id: str = transaction["message"]["program_id"]
            account_ids: list[str] = transaction["message"]["account_ids"]
            yield {
                "hash": transaction["hash"],
                "program_id": program_id,
                "accounts": len(account_ids),
                "account_ids": account_ids,
                "block_id": block["block_id"],
                "block_hash": block["hash"],
                "block_timestamp": block["timestamp"],
                "timezone": block["timezone"]
            }

class PrivacyPreservingTransactions(BlockSubStream):

    primary_key = "hash"

    def records_from_block(self, block: Mapping[str, Any], transactions: list) -> Iterable[Mapping[str, Any]]:
        for current in transactions:
            transaction: dict = current.get("PrivacyPreserving") or {}
            if not transaction:
                continue

            public_actions: list[dict] = transaction["message"]["public_actions"]
            yield {
                "hash": transaction["hash"],
                "public_actions": public_actions,
                "block_id": block["block_id"],
                "block_hash": block["hash"],
                "block_timestamp": block["timestamp"],
                "timezone": block["timezone"]
            }

class ProgramDeploymentTransactions(BlockSubStream):

    primary_key = "hash"

    def records_from_block(self, block: Mapping[str, Any], transactions: list) -> Iterable[Mapping[str, Any]]:
        for current in transactions:
            transaction: dict = current.get("ProgramDeployment") or {}
            if not transaction:
                continue

            yield {
                "hash": transaction["hash"],
                "block_id": block["block_id"],
                "block_hash": block["hash"],
                "block_timestamp": block["timestamp"],
                "timezone": block["timezone"]
            }

class SourceLogosExecutionZone(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:

        rpc_method = "checkHealth"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": rpc_method,
            "params": [],
        }
        headers = {
            "Content-Type": "application/json"
        }
        data = json.dumps(payload).encode("utf-8")
        logger.info(f"[POST] URL: {config['url']}")
        logger.info(f"Payload: {payload}")
        response = requests.post(config["url"], data=data, headers=headers)
        success = response.status_code == 200

        return success, None if success else f"Could not connect to LEZ [{response.status_code}]"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        params = {
            "url_base": config["url"],
            "rpc_method": "getBlockById",
            "latest_block_id": self.get_final_block(config["url"])
        }
        cache = {}
        streams = [
            BlockInfo(**params, cache=cache),
            PublicTransactions(**params, cache=cache),
            PrivacyPreservingTransactions(**params, cache=cache),
            ProgramDeploymentTransactions(**params, cache=cache)
        ]
        return streams

    def get_final_block(self, url: str) -> int:
        """
        Get the latest finalized block ID from LEZ.

        Parameters:
            - `url` - LEZ url

        Output:
            - the latest LEZ block ID
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLastFinalizedBlockId",
            "params": [],
        }
        headers = {
            "Content-Type": "application/json"
        }
        data = json.dumps(payload).encode("utf-8")
        response = requests.post(url, data=data, headers=headers)
        block_id = response.json().get("result")
        if not block_id:
            raise Exception("Could not fetch last finalized LEZ block...")

        return block_id
