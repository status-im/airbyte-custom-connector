import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceGithubMarkdownFetcher

def run():
    source = SourceGithubMarkdownFetcher()
    launch(source, sys.argv[1:])
