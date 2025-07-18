import sys
import logging
from airbyte_cdk.entrypoint import launch
from .source import SourceRedditFetcher

def run():

    logger = logging.getLogger("airbyte")
    args = sys.argv[1:]
    
    source = SourceRedditFetcher()
    launch(source, sys.argv[1:])