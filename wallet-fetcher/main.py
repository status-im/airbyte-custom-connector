#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_wallet_fetcher import SourceWalletFetcher

if __name__ == "__main__":
    source = SourceWalletFetcher()
    launch(source, sys.argv[1:])
