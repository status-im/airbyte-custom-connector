import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceElasticsearchQuery


def run():
    source = SourceElasticsearchQuery()
    launch(source, sys.argv[1:])
