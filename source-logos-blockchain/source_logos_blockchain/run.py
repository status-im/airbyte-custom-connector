import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceLogosBlockchain

def run():
    source = SourceLogosBlockchain()
    launch(source, sys.argv[1:])
