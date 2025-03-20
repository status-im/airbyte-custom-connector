import sys
import logging
from airbyte_cdk.entrypoint import launch, AirbyteEntrypoint
from .source import SourceBlockchainExplorer

def run():

    logger = logging.getLogger("airbyte")
    args = sys.argv[1:]
    
    source = SourceBlockchainExplorer()
    launch(source, sys.argv[1:])

    if "read" not in args:
        return
    
    config_path = AirbyteEntrypoint(source).extract_config(args)
    config = source.read_config(config_path)

    debug_mode = config["blocks_to_do"] > 0

    if debug_mode:
        logger.info(f"Processed {config['blocks_to_do']} block pages")
        return
    
    source.set_block(source.blocks.starting_block)
    logger.info(f"{__name__}(): Updated text file {source.starting_block_path}")