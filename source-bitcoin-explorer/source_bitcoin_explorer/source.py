from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from .stream import Transactions
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

logger = logging.getLogger("airbyte")
# Source
class SourceBitcoinExplorer(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:

        failed = []
        for wallet in config["wallets"]:

            url = "https://blockchain.info/rawaddr/" + wallet["address"]
            response = requests.get(url)

            if response.status_code != 200:
                failed.append(f"Could not make REST GET request {response.status_code} - {url}")
                continue
            logger.info(f"Successfully connected to {url}")

        return len(failed) == 0, "\n".join(failed) if failed else None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            Transactions(wallets=config['wallets'])
        ]
