from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
import requests
import logging
import os

logger = logging.getLogger("airbyte")

class ApiStream(HttpStream):

    primary_key: Optional[str] = None

    def __init__(self, url: str):
        super().__init__()
        self.__url_base = url + ("api/v2" if url.endswith("/") else "/api/v2")

    @property
    def url_base(self) -> str:
        return self.__url_base

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None



class Stats(ApiStream):

    def __init__(self, url: str):
        super().__init__(url)

    def path(self, **kwargs):
        return f"{self.url_base}/stats"
        
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        yield data



class Blocks(ApiStream):

    def __init__(self, url: str, starting_block: int):
        super().__init__(url)
        self.__starting_block = starting_block
        logger.info(f"starting_block: {self.starting_block}")



    @property
    def starting_block(self) -> int:
        return self.__starting_block



    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:

        data: dict = response.json()
        next_page_params = data.get("next_page_params")        
        items = data.get("items")

        if not items:
            return None
        
        biggest_block = items[0]["height"]
        smallest_block = items[-1]["height"]

        if smallest_block == 0 or (isinstance(self.__starting_block, int) and biggest_block <= self.__starting_block):
            return None
        
        return next_page_params



    def path(self, **kwargs):
        url = f"{self.url_base}/blocks"
        return url



    def request_params(self, stream_state: Optional[dict[str, Any]], stream_slice: Optional[dict[str, Any]] = None, next_page_token: Optional[dict[str, Any]] = None):
        return next_page_token



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        items: Optional[list[dict]] = data.get("items")
        largest_block: int = items[0]["height"]
        
        for item in items:

            if item["height"] <= self.__starting_block:
                logger.info(f"Stopping execution - item[\"height\"] is  {item['height']} and the starting_block is {self.__starting_block}")
                break
            
            yield item

        smallest_block = items[-1]['height']
        logger.info(f"Uploaded block range {smallest_block} to {largest_block}")
        
        if self.__starting_block < largest_block:
            logger.info(f"Updated starting_block from {self.__starting_block} to {largest_block} ({largest_block - self.__starting_block} blocks difference)")
            self.__starting_block = largest_block 



class Transactions(HttpSubStream, ApiStream):
    
    def __init__(self, url: str, parent: HttpStream):
        super().__init__(parent=parent, url=url)



    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        
        block: dict = stream_slice.get("parent")
        block_id = block["hash"]
        url = f"{self.url_base}/blocks/{block_id}/transactions"
        return url



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        data: dict = response.json()
        items: Optional[list[dict]] = data.get("items")

        logger.info(f"URL: {response.url}")
        for item in items:
            yield item 



class SourceStatusNetworkStats(AbstractSource):

    # Store the current state of Airbyte
    config_file_path: str = "/data/overwrite-config.json"

    def __init__(self):
        super().__init__()
        self.__blocks: Optional[Blocks] = None
        self.__config: Optional[Mapping[str, Any]] = None


    @property
    def blocks(self) -> Blocks:

        if not self.__blocks:
            raise Exception("Variable cannot be used before airbyte_cdk.entrypoint.launch runs")
        
        return self.__blocks



    @property
    def config(self) -> Mapping[str, Any]:

        if not self.__config:
            raise Exception("Variable cannot be used before airbyte_cdk.entrypoint.launch run. Please check if a config file has been created")

        return self.__config
    


    def check_connection(self, logger: logging.Logger, config: dict) -> Tuple[bool, Any]:
        
        response = requests.get(config["url_base"] + "/stats")

        success = True
        message = f"Successfully connected to {config['url_base']}"
        try:
            response.raise_for_status()
        except Exception as e:
            success = False
            message = str(e)

        return success, message



    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        
        params = {
            "url": config["url_base"]
        }
        
        if not os.path.exists(self.config_file_path):
            os.makedirs(os.path.dirname(self.config_file_path), exist_ok=True)
            self.write_config(config, self.config_file_path)
            logger.info(f"Created config in {self.config_file_path}")
        else:
            config = self.read_config(self.config_file_path)
            logger.info(f"Loaded config file from volume - {self.config_file_path}")

        self.__config = config
        logger.info(f"{self.__class__.__name__} config: {config}")

        self.__blocks = Blocks(**params, starting_block=config["starting_block"])
        return [Stats(**params), self.__blocks, Transactions(parent=self.__blocks, **params)]