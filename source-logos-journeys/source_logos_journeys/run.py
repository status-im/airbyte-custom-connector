import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceLogosJourneys


def run():
    source = SourceLogosJourneys()
    launch(source, sys.argv[1:])
