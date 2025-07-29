import sys
from airbyte_cdk.entrypoint import launch
from .source import SourceRedditFetcher

def run():
    source = SourceRedditFetcher()
    launch(source, sys.argv[1:])
