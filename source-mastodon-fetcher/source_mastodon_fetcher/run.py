import sys
from airbyte_cdk.entrypoint import launch
from .source import SourceMastodonFetcher

def run():
    source = SourceMastodonFetcher()
    launch(source, sys.argv[1:])