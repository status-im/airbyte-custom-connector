# Blockchain Info Fetcher Source

This is the directory for the Bitcoin Wallet source connector, written in Python.

## Usage

This connector fetch Bitcoin wallet balance from [blockchain.info](https://blockain.info).


### Configuration

The connector takes the following input:

```yaml
wallets:
  address:
    title: Address
    type: string
    description: Address of the wallet
  name:
    title: Name
    type: string
    description: Name of the wallet
  tags:
    title: tags
    type: string
    description: List of tags linked to the wallet
  backfill: 
    title: Backfill
    type: boolean
    description: If True then the entire history will be fetched. If False only yesterday's prices will be fetched
```



### Output

This connector will return a list of coin for each chain with the following models

- `timestamp` - The transaction timestamp
- `timezone` - UTC by default
- `hash` - transaction hash
- `chain` - **bitcoin**. This makes the data a bit more consistent with [Etherscan](https://etherscan.io/) chains.
- `chain_id` - `None`. This makes the data a bit more consistent with [Etherscan](https://etherscan.io/) chains.
- `wallet_name` - Name of the wallet so it is easier to search.
- `wallet_address` - Address of the wallet.
- `tags` - Tags associated with the wallet owning the token.
- `total_transactions` - Total number of transactions the address has done.
- `total_received` - Total Satoshi (SAT) received in the account
- `total_sent` - Total Satoshi (SAT) sent from the account
- `token_name` - **Bitcoin**. This makes the data a bit more consistent with [Etherscan](https://etherscan.io/) chains.
- `token_symbol` - **BTC**. This makes the data a bit more consistent with [Etherscan](https://etherscan.io/) chains.
- `token_decimal` - The divisor to convert Satoshi (SAT) to Bitcoin (BTC).
- `transaction_fee` - The transaction fee in Satoshi (SAT).
- `net_change` - `sent - received`.
- `movement` - Check if money is coming in the account or out of the account.
- `current_balance` - Balance after the current transaction.
- `sent` - How many Satoshi (SAT) was sent from the account for the current transaction.
- `received` - How many Satoshi (SAT) was received in the account for the current transaction.
- `from_utxo` - the UTXOs who sent the Satoshi (SAT)
- `to_utxo` - the UTXOs that received the Satoshi (SAT)


## Local development

### Prerequisites

#### Activate Virtual Environment and install dependencies

From this connector directory, create a virtual environment:

```
python -m venv .venv
```

```
source .venv/bin/activate
pip install -r requirements.txt
```

### Locally running the connector

```
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/source-bitcoin-explorer:dev .
# Running the spec command against your patched connector
docker run airbyte/source-bitcoin-explorer:dev spec
```

Then run any of the connector commands as follows:

#### Linux / MAC OS

```
docker run --rm airbyte/source-bitcoin-explorer:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bitcoin-explorer:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bitcoin-explorer:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-bitcoin-explorer:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json  
```

#### Windows

```
docker run --rm airbyte/source-bitcoin-explorer:dev spec
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-bitcoin-explorer:dev check --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-bitcoin-explorer:dev discover --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-bitcoin-explorer:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```