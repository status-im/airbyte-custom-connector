import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceTelegramFetcher


def run():
    source = SourceTelegramFetcher()
    launch(source, sys.argv[1:])
