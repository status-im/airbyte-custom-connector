#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceGoogleStore

def run():
    source = SourceGoogleStore()
    launch(source, sys.argv[1:])
