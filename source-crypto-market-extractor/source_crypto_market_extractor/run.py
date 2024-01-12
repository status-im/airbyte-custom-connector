#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .source import SourceCryptoMarketExtractor

def run():
    source = SourceCryptoMarketExtractor()
    launch(source, sys.argv[1:])
