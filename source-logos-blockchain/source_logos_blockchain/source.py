import requests, logging
from urllib.parse import urlparse, parse_qsl
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

class LogosBlockchainStream(HttpStream):

    cursor_field = "slot"
    primary_key = "id"

    def __init__(self, url_base: str, latest_slot: int, pagination: int):
        super().__init__()
        self.__url_base = url_base
        self.latest_slot = latest_slot
        self.pagination = pagination

    @property
    def url_base(self):
        return self.__url_base

    def path(self, **kwargs) -> str:
        return "/cryptarchia/blocks"

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None) -> MutableMapping[str, Any]:

        slot_from = stream_state.get("slot", 0)
        slot_to = slot_from + self.pagination

        params = {
            "slot_from": slot_from if not next_page_token else next_page_token["slot_from"],
            "slot_to": slot_to if not next_page_token else next_page_token["slot_to"]
        }
        self.logger.info(f"{self.name} > params: {params}")
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"slot": latest_record["slot"]}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # Response JSON does not return any pagination information
        # Getting the highest slot from response.json() may return 0 blocks
        # It is safer to extract the current request params from the URL
        params = self.extract_params(response)
        slot_from = int(params["slot_to"]) + 1
        slot_to = min(slot_from + self.pagination, self.latest_slot)
        params = {
            "slot_from": slot_from,
            "slot_to": slot_to
        }
        if slot_to == self.latest_slot:
            params = None
            self.logger.info(f"{self.name} > No more extraction")
        else:
            self.logger.info(f"{self.name} > params: {params}")

        return params

    def extract_params(self, response: requests.Response) -> dict:
        """
        Extract the URL GET parameters from the request.

        Parameters:
            - `response` - the Airbyte response

        Output:
            - GET parameters
        """
        parsed = urlparse(response.url)
        params = parse_qsl(parsed.query)
        return {k: v for k, v in params}

class LogosBlockStream(LogosBlockchainStream):

    def __init__(self, url_base: str, latest_slot: int, pagination: int):
        super().__init__(url_base, latest_slot, pagination)

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        # Blocks are returned in ascending order
        blocks: list[dict] = response.json()
        for block in blocks:
            transactions = block.pop("transactions")
            point = {
                **block["header"],
                "transactions": len(transactions)
            }
            yield point

class LogosTransactionsStream(LogosBlockchainStream):

    def __init__(self, url_base: str, latest_slot: int, pagination: int):
        super().__init__(url_base, latest_slot, pagination)

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        # Blocks are returned in ascending order
        blocks: list[dict] = response.json()
        for block in blocks:
            transactions: list[dict] = block.pop("transactions")
            for transaction in transactions:
                point = {
                    "block_id": block["header"]["id"],
                    "slot": block["header"]["slot"],
                    **transaction,
                }
                yield point

class SourceLogosBlockchain(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        info = self.get_blockchain_info(config)
        is_online = info["mode"].lower() == "online"
        return is_online, None if is_online else f"Logos Node [{config['url']}] is not online..."

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        info = self.get_blockchain_info(config)
        params = {
            "url_base": self.parse_url(config["url"]),
            "latest_slot": info["slot"],
            "pagination": config["slot_pagination"]
        }
        streams = [
            LogosBlockStream(**params),
            LogosTransactionsStream(**params),
        ]
        return streams

    def get_blockchain_info(self, config: Mapping[str, Any]) -> dict:
        url = self.parse_url(config["url"]) + "/cryptarchia/info"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def parse_url(self, url: str) -> str:
        url = url[:-1] if url.endswith("/") else url
        return url.replace("\"", "").replace("\'", "")
