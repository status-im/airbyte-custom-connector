from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from . import constants
import requests
import logging

logger = logging.getLogger("airbyte")

class ApiStream(HttpStream):

    url_base: str = constants.URL_BASE
    primary_key: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


class Stats(ApiStream):

    def path(self, *args, **kwargs):
        return f"{self.url_base}/stats"
        
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        yield data



class Blocks(ApiStream):

    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:
        data: dict = response.json()
        next_page_params = data.get("next_page_params")
        return None
        items = data.get("items")
        if not items:
            return None
        
        smallest_block = items[-1]["height"]
        if smallest_block == 0:
            return None
        
        return next_page_params



    def path(self, **kwargs):
        url = f"{self.url_base}/blocks"
        return url



    def request_params(self, stream_state: Optional[dict[str, Any]], stream_slice: Optional[dict[str, Any]] = None, next_page_token: Optional[dict[str, Any]] = None):
        # TO DO: Add stream_state when backlog finishes
        return next_page_token



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        items: Optional[list[dict]] = data.get("items")
        
        for item in items:
            yield item



class Transactions(HttpSubStream, ApiStream):
    

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        
        block = stream_slice.get("parent")
        logger.info(f"Transaction -> path: {block}")

        block_id = block["hash"]
        url = f"{self.url_base}/blocks/{block_id}/transactions"
        return url
    
    def parse_response(self, 
                       response: requests.Response, 
                       *, 
                       stream_state: Optional[dict[str, Any]], 
                       stream_slice: Optional[dict[str, Any]] = None, 
                       **kwargs
                    ) -> Iterable[Mapping]:
        logger.info(f"Transactions -> parse_response: {stream_state}")
        
        yield {}





class SourceStatusNetworkStats(AbstractSource):

    def check_connection(self, logger: logging.Logger, config: dict) -> Tuple[bool, Any]:
        response = requests.get(constants.URL_BASE + "/stats")

        success = True
        message = f"Successfully connected to {constants.URL_BASE}"
        try:
            response.raise_for_status()
        except Exception as e:
            success = False
            message = str(e)

        return success, message

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        blocks = Blocks()
        return [Stats(), blocks, Transactions(parent=blocks)]