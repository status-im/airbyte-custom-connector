import sys
from pprint import pprint
from airbyte_cdk.entrypoint import launch
from .source import SourceStatusNetworkStats

def run():
    source = SourceStatusNetworkStats()
    launch(source, sys.argv[1:])