import sys
import logging
from typing import Mapping, Any
from airbyte_cdk.entrypoint import launch
from .source import SourceStatusNetworkStats
from airbyte_cdk.connector import BaseConnector

def run():
    source = SourceStatusNetworkStats()
    launch(source, sys.argv[1:])

    logger = logging.getLogger("airbyte")
    config: Mapping[str, Any] = BaseConnector.read_config(source.config_path)
    logger.info(f"Opened {source.config_path}")
    config["starting_block"] = source.blocks.starting_block

    BaseConnector.write_config(config, config["config_path"])
    logger.info(f"Updated config {config}")