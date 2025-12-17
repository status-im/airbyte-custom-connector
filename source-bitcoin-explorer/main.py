#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_bitcoin_explorer import SourceBitcoinExplorer

if __name__ == "__main__":
    source = SourceBitcoinExplorer()
    launch(source, sys.argv[1:])
