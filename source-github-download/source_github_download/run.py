import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceGithubDownload

def run():
    source = SourceGithubDownload()
    launch(source, sys.argv[1:])
