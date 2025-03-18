from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, CheckpointMixin
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.models.airbyte_protocol import SyncMode
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
    
    def __init__(self, url: str, starting_block: int = 0):
        super().__init__(url)
        self.__starting_block = starting_block
        self.__highest_block = starting_block       
        self.__class_name = self.__class__.__name__
        self.completed = 0
        
    @property  
    def use_cache(self) -> bool:  
        return True 

    @property
    def starting_block(self) -> int:
        return self.__starting_block



    @property
    def highest_block(self) -> int:
        return self.__highest_block



    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:

        data: dict = response.json()
        next_page_params = data.get("next_page_params")        
        items = data.get("items")

        if not items or self.completed == 3:
            logger.info(f"{self.__class_name}: self.completed = {self.completed}")
            return None
        
        self.completed += 1

        biggest_block = items[0]["height"]
        smallest_block = items[-1]["height"]

        if smallest_block == 0 or (isinstance(self.__starting_block, int) and biggest_block <= self.starting_block):
            logger.info(f"smallest_block: {smallest_block}")
            logger.info(f"biggest_block: {biggest_block}")
            logger.info(f"starting_block: {self.starting_block}")
            logger.info(f"highest_block: {self.highest_block}")
            self.__starting_block = self.__highest_block
            return None
        
        return next_page_params



    def path(self, **kwargs):
        url = f"{self.url_base}/blocks"
        return url



    def request_params(self, stream_state: Optional[dict[str, Any]], stream_slice: Optional[dict[str, Any]] = None, next_page_token: Optional[dict[str, Any]] = None):
        
        logger.info(f"{self.__class_name}: stream_state: {stream_state}")
        logger.info(f"{self.__class_name}: stream_slice: {stream_slice}")
        logger.info(f"{self.__class_name}: next_page_token: {next_page_token}")

        return next_page_token



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        items: Optional[list[dict]] = data.get("items")
        largest_block: int = items[0]["height"]
        
        for item in items:

            if item["height"] <= self.__starting_block:
                logger.info(f"Stopping execution - item[\"height\"] is  {item['height']} and the starting_block is {self.__starting_block}")
                break

            item = {**item, "block_batch": self.completed}
            yield item

        smallest_block = items[-1]['height']
        logger.info(f"Uploaded block range {smallest_block} to {largest_block}")
        
        if self.__highest_block < largest_block:
            logger.info(f"Updated highest_block from {self.highest_block} to {largest_block} ({largest_block - self.__highest_block} blocks difference)")
            self.__highest_block = largest_block 



class Transactions(HttpSubStream, ApiStream):
    
    def __init__(self, url: str, parent: Blocks):
        super().__init__(parent=parent, url=url)
        self.__class_name = self.__class__.__name__



    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        logger.info(f"{self.__class_name} path: {stream_slice}")
        block: dict = stream_slice.get("parent")
        block_id = block["hash"]        
        url = f"{self.url_base}/blocks/{block_id}/transactions"
        return url



    def stream_slices(self, sync_mode: SyncMode, cursor_field: Optional[list[str]] = None, stream_state: Optional[Mapping[str, Any]] = None) -> Iterable[Mapping]:
        # Re-iterate over parent's records to capture new blocks
        parent_records = self.parent.read_records(
            sync_mode = sync_mode, 
            stream_state = stream_state
        )
        for parent_record in parent_records:
            logger.info(f"{self.__class_name}.get_stream_slices: {parent_record}")
            yield {"parent": parent_record}



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        data: dict = response.json()
        items: Optional[list[dict]] = data.get("items", [])

        logger.info(f"{self.__class_name} URL: {response.url}")
        logger.info(f"{self.__class_name} transactions: {len(items)}")

        url = response.url
        start_prefix = "/blocks/"
        start = url.index(start_prefix) + len(start_prefix)

        end_prefix = "/transactions"
        end = url.index(end_prefix)

        block_id = url[start:end]
        for item in items:
            yield {**item, "block_id": block_id}



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

        self.__blocks = Blocks(**params)
        return [Stats(**params), self.__blocks, Transactions(parent=self.__blocks, **params)]