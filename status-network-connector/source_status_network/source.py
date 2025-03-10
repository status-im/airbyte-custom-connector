from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
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

    def path(self, **kwargs):
        url = f"{self.url_base}/blocks"
        return url
    
    def parse_response(self, response: requests.Response, **kwargs):
        data: dict[str, Any] = response.json()

        items = data.get("items")
        next_page_params = data.get("next_page_params")
        counter = 0
        smallest_block = -1

        while smallest_block != 0:
            
            for item in items:
                yield item
            
            smallest_block = item["height"]
            logger.info(f"Current smallest block height:\t{smallest_block}")

            url = self.path() + f"?block_number={next_page_params['block_number']}"
            logger.info(f"Fetching data for {url}")
            data = requests.get(url).json()

            items: Optional[list[dict]] = data.get("items")
            next_page_params: Optional[dict] = data.get("next_page_params")

            # Used for quick testing
            counter += 1
            if counter == 1:
                break



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
        return [Stats(), Blocks()]