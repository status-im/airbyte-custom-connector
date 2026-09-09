import requests, logging, json, datetime
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

    def next_page_token(self, response):
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

class LEZBlocks(LogosExecutionZoneStream):

    def __init__(self, url_base: str, rpc_method: str, latest_block_id: int):
        super().__init__(url_base, rpc_method, latest_block_id)

    def request_body_json(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None):
        data = super().request_body_json(stream_state, stream_slice, next_page_token)
        data["params"] = [stream_slice.get("block_id", 1)]
        return data

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):

        data = response.json()["result"]
        transactions: list[dict] = data["body"]["transactions"]
        total_transactions = len(transactions)
        public_transactions = sum(["Public" in transaction.keys() for transaction in transactions])
        private_transactions = total_transactions - public_transactions

        point = {
            "block_id": data["header"]["block_id"],
            "hash": data["header"]["hash"],
            "prev_block_hash": data["header"]["prev_block_hash"],
            "timestamp": datetime.datetime.fromtimestamp(data["header"]["timestamp"] / 1_000),
            "timezone": "UTC",
            "total_transactions": len(transactions),
            "public_transactions": public_transactions,
            "private_transactions": private_transactions,
            "status": data["bedrock_status"],
            "rpc_method": stream_slice["rpc_method"]
        }

        yield point

class LEZTransactions(LEZBlocks):

    def __init__(self, url_base: str, rpc_method: str, latest_block_id: int):
        super().__init__(url_base, rpc_method, latest_block_id)

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        block_data = next(super().parse_response(response, stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token))

        data = response.json()["result"]
        transactions: list[dict] = data["body"]["transactions"]

        for current in transactions:
            for transaction_type, transaction in current.items():
                account_ids: list[str] = transaction["message"].get("account_ids")
                if not isinstance(account_ids, list):
                    continue

                point = {
                    "hash": transaction["hash"],
                    "type": transaction_type.lower(),
                    "accounts": len(account_ids),
                    "account_ids": account_ids,
                    "block_id": block_data["block_id"],
                    "block_hash": block_data["hash"],
                    "block_timestamp": block_data["timestamp"],
                    "timezone": block_data["timezone"]
                }
                yield point

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
        streams = [
            LEZBlocks(**params),
            LEZTransactions(**params)
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
