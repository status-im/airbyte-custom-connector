# Etherscan Source

## Usage

This Airbyte connector extracts data from the Etherscan API.

- `WalletTransactions` -  Etherscan **Transactions** tab
- `WalletInternalTransactions` - Etherscan **Internal Transactions** tab
- `WalletTokenTransactions` - Etherscan **Token Transfers (ERC-20)** tab
- `MinedBlocks` - Etherscan **Beacon Chain > Produced Blocks** tab
- `BeaconWithdrawals` - Etherscan **Beacon Chain > Withdrawals** tab
- `NativeBalance` - `ETH` balance as in the **Overview**
- `TokenBalance` - ERC-20 token balance as in **TOKEN HOLDINGS**

## Data Backfill

To fetch all of the transactions and beacon chain data from the beginning, you must:

1. Set the `backfill` config variable to `True`
2. Delete / drop the Airbyte tables and schema (**Optional**)
3. Make sure that the Sync mode is `Full refresh | Append` mode, instead of `Full refresh | Overwrite`. **Overwrite** will cause the streams to run in parallel instead of sequentially.

## Local development

### Prerequisites

From this connector directory, create a virtual environment:
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Locally running the connector
```bash
poetry run source-etherscan spec
poetry run source-etherscan check --config secrets/config.json
poetry run source-etherscan discover --config secrets/config.json
poetry run source-etherscan read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/source-etherscan:dev .
# Running the spec command against your patched connector
docker run airbyte/source-etherscan:dev spec
```

#### Run
Then run any of the connector commands as follows:
```bash
docker run --rm airbyte/source-etherscan:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-etherscan:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-etherscan:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-etherscan:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```
