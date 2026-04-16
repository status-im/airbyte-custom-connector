import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceGithubSearchCount


def run():
    source = SourceGithubSearchCount()
    launch(source, sys.argv[1:])
