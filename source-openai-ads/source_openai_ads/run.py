import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceOpenAIAds


def run():
    source = SourceOpenAIAds()
    launch(source, sys.argv[1:])
