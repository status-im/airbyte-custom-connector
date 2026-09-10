import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceLogosExecutionZone

def run():
    source = SourceLogosExecutionZone()
    launch(source, sys.argv[1:])
