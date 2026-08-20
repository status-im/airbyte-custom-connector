import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceLogosTestnetLogs


def run():
    source = SourceLogosTestnetLogs()
    launch(source, sys.argv[1:])
