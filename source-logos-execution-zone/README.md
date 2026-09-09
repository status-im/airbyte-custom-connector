# Logos Execution Zone Source
    
This connector is experimental and will change in future, when Logos moves to mainnet.

## Usage

This Airbyte connector extracts data from a **[Logos Execution Zone (LEZ)](https://github.com/logos-blockchain/logos-execution-zone/)**.

## Data extracted

The connector walks the LEZ chain block by block (via the `getBlockById` JSON-RPC method), from the last synced block up to the latest finalized block (`getLastFinalizedBlockId`), and produces two streams:

### `blocks`

One record per block, with aggregated transaction counts:

| Field | Description |
| --- | --- |
| `block_id` | Block height / ID (cursor field for incremental sync) |
| `hash` | Block hash (primary key) |
| `prev_block_hash` | Hash of the previous block |
| `timestamp` | Block time, converted from epoch milliseconds |
| `timezone` | Always `UTC` |
| `total_transactions` | Number of transactions in the block |
| `public_transactions` | Count of public transactions |
| `private_transactions` | Count of private transactions (`total - public`) |
| `status` | Bedrock status of the block. Should always be `Finalized`, since the connector only syncs up to the last finalized block (`getLastFinalizedBlockId`). Be careful when processing the latest block - if it is not yet finalized this value will differ. |
| `rpc_method` | RPC method used to fetch the block. This field is used for debugging purposes. |

### `transactions`

One record per transaction (only transactions carrying a list of `account_ids` are emitted):

| Field | Description |
| --- | --- |
| `hash` | Transaction hash (primary key) |
| `type` | Transaction type (`public` / `private`) |
| `accounts` | Number of accounts involved in the transaction. |
| `account_ids` | Account IDs referenced by the transaction |
| `block_id` | ID of the containing block |
| `block_hash` | Hash of the containing block |
| `block_timestamp` | Timestamp of the containing block |
| `timezone` | Always `UTC` |

## Local development

To run the Airbyte connector locally:

1. Make sure [Logos Execution Zone](https://docs.logos.co/lez/get-started/quickstart-for-the-logos-execution-zone-wallet) is set up and [running](https://docs.logos.co/lez/follow-chain/run-lez-indexer).

2. By default, the Logos Execution Zone node does not expose its IP address externally. For example, if LEZ is running on a Raspberry Pi and the Airbyte connector is running on a different device, you'll need to establish an `ssh` tunnel: `ssh -N -L 8779:localhost:8779 your-username@ip-address`.

3. Update the [config](./sample_files/config-example.json) `url` field to `http://host.docker.internal:8779/`.

### Prerequisites

From this connector directory, create a virtual environment:
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Locally running the connector
```bash
poetry run source-logos-execution-zone spec
poetry run source-logos-execution-zone check --config secrets/config.json
poetry run source-logos-execution-zone discover --config secrets/config.json
poetry run source-logos-execution-zone read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/source-logos-execution-zone:dev .
# Running the spec command against your patched connector
docker run airbyte/source-logos-execution-zone:dev spec
```

#### Run
Then run any of the connector commands as follows:
```bash
docker run --rm airbyte/source-logos-execution-zone:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-logos-execution-zone:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-logos-execution-zone:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-logos-execution-zone:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```
