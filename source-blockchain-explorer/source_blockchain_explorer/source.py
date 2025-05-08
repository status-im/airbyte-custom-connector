from typing import Any, Iterable, List, Mapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.models.airbyte_protocol import SyncMode
import time
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

    def __init__(self, url: str, **kwargs):
        super().__init__(url)

    def path(self, **kwargs):
        return f"{self.url_base}/stats"
        
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data: dict[str, Any] = response.json()
        yield data



class Blocks(ApiStream):
    
    def __init__(self, url: str, starting_block: int, blocks_to_do: int = 0, limit: int = 9):
        """
        Parameters:
            - `url` - the base URL of the API
            - `starting_block` - the block that will terminate the REST API pagination.
                                 NOTE: You start from the current block on the blockchain 
                                 and go traverse backwards until `starting_block` is reached
            - `blocks_to_do` - number of block pages that will be fetched. If 0, then all missing
                               blocks will be fetched. Put a POSITIVE value to
            - `limit` - the number of REST API requests that can be made before a small pause is put
        """
        super().__init__(url)
        self.__starting_block = starting_block
        self.__highest_block = starting_block       
        self.__class_name = self.__class__.__name__
        self.completed = 0
        self.blocks_to_do = blocks_to_do
        
        if blocks_to_do > 0:
            logger.info(f"Blocks to do: {blocks_to_do}")
        
        self.limit = limit
        self.count = 0



    @property  
    def use_cache(self) -> bool:  
        return True 



    @property
    def starting_block(self) -> int:
        """
        Custom Airibyte variable
        """
        return self.__starting_block



    @property
    def highest_block(self) -> int:
        """
        Custom Airibyte variable
        """
        return self.__highest_block



    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:

        data: dict = response.json()
        next_page_params = data.get("next_page_params")        
        items = data.get("items")

        debug_mode = self.completed > self.blocks_to_do and self.blocks_to_do > 0
        if not items or debug_mode:
            message = f"no 'items' in 'next_page_params' to process" if not debug_mode else f"DEBUG MODE. Completed {self.blocks_to_do} blocks"
            logger.info(f"{self.__class_name}: {message}")
            return None
        
        self.completed += 1

        biggest_block = items[0]["height"]
        smallest_block = items[-1]["height"]

        if smallest_block == 0 or biggest_block <= self.__starting_block:
            logger.info(f"starting_block: {self.starting_block}")
            logger.info(f"highest_block: {self.highest_block}")
            self.__starting_block = self.__highest_block
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
        logger.info(f"Processed {largest_block - smallest_block} blocks ({smallest_block} to {largest_block})")
        
        if self.__highest_block < largest_block:
            logger.info(f"Updated highest_block from {self.highest_block} to {largest_block} ({largest_block - self.__highest_block} blocks difference)")
            self.__highest_block = largest_block 

        self.count += 1

        if self.count == self.limit:
            logger.info(f"Made {self.count} requests. Sleeping for 1s")
            self.count = 0
            time.sleep(1)



class Transactions(HttpSubStream, ApiStream):
    
    def __init__(self, **kwargs):
        super().__init__(parent=Blocks, url=kwargs["url"])
        self.parent = Blocks(**kwargs)
        self.__class_name = self.__class__.__name__
        self.limit = kwargs["limit"]
        self.count = 0


    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        block: dict = stream_slice.get("parent")
        block_id = block["hash"]        
        url = f"{self.url_base}/blocks/{block_id}/transactions"
        return url



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        data: dict = response.json()
        items: Optional[list[dict]] = data.get("items", [])

        url = response.url
        start_prefix = "/blocks/"
        start = url.index(start_prefix) + len(start_prefix)

        end_prefix = "/transactions"
        end = url.index(end_prefix)

        block_id = url[start:end]
        logger.info(f"{self.__class_name}: Block {block_id} has {len(items)} transactions")

        for item in items:
            yield {**item, "block_hash": block_id}

        self.count += 1

        if self.count == self.limit:
            logger.info(f"Made {self.count} requests. Sleeping for 1s")
            self.count = 0
            time.sleep(1)


class SourceBlockchainExplorer(AbstractSource):

    # Store the current block for the next Airbyte run
    starting_block_path: str = "/data/starting_block.txt"

    def __init__(self):
        super().__init__()
        self.__blocks: Optional[Blocks] = None
        


    @property
    def blocks(self) -> Blocks:
        """
        Custom Airibyte variable
        """
        if not self.__blocks:
            raise Exception("Variable cannot be used before airbyte_cdk.entrypoint.launch runs")
        
        return self.__blocks



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
            "url": config["url_base"],
            "limit": 9 # Based on https://docs.blockscout.com/setup/env-variables/backend-env-variables#api-rate-limits (API_RATE_LIMIT)
        }
        
        starting_block = 0
        if not os.path.exists(self.starting_block_path):            
            self.set_block(starting_block)
            logger.info(f"Created text file {self.starting_block_path}")
        else:
            starting_block = self.get_block()            
            logger.info(f"Loaded text file from volume - {self.starting_block_path}")

        logger.info(f"starting_block: {starting_block}")

        blocks_params = {
            **params,
            "starting_block": starting_block, 
            "blocks_to_do": config["blocks_to_do"]
        }
        self.__blocks = Blocks(**blocks_params)
        return [Stats(**params), self.__blocks, Transactions(**blocks_params)]
    


    def get_block(self) -> int:
        """
        Custom Airbyte function. Get the latest block that 
        has been completed uploaded from the previous Airbyte run

        Output:
            - The last processed block
        """

        if not os.path.exists(self.starting_block_path):
            return 0

        with open(self.starting_block_path, "r") as f:
            starting_block = int(f.read())

        return starting_block
    


    def set_block(self, value: int):
        """
        Custom Airbyte function. Write the given value (highest processed block)
        so the next Airbyte run can pick up from where the current run finished.

        Parameters:
            - `value` - the highest processed block within the current Airbyte run
        """
        # So local Docker development / new instance runs do not break
        os.makedirs(os.path.dirname(self.starting_block_path), exist_ok=True)

        with open(self.starting_block_path, "w") as f:
            f.write(str(value))