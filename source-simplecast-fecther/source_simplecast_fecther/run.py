#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceSimplecastFecther

def run():
    source = SourceSimplecastFecther()
    launch(source, sys.argv[1:])
