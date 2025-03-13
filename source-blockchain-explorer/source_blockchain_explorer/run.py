import sys
import logging
from airbyte_cdk.entrypoint import launch
from .source import SourceStatusNetworkStats

def run():

    logger = logging.getLogger("airbyte")
    args = sys.argv[1:]
    
    source = SourceStatusNetworkStats()
    launch(source, sys.argv[1:])

    if "read" not in args:
        return

    logger.info(f"Opened {source.config_file_path}")
    source.config["starting_block"] = source.blocks.starting_block

    source.write_config(source.config, source.config_file_path)
    logger.info(f"{__name__}(): Updated config {source.config}")