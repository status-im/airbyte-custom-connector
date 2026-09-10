# Logos Execution Zone Source
    
This connector is experimental and will change in future, when Logos moves to mainnet.

## Usage

This Airbyte connector extracts data from a **[Logos Execution Zone (LEZ)](https://github.com/logos-blockchain/logos-execution-zone/)**.

## Data extracted

The connector walks the LEZ chain block by block (via the `getBlockById` JSON-RPC method), from the last synced block up to the latest finalized block (`getLastFinalizedBlockId`), and produces two streams:

### `block_info`

One record per block, with aggregated transaction counts:

| Field | Description |
| --- | --- |
| `block_id` | Sequential block height / ID |
| `hash` | Block hash |
| `prev_block_hash` | Hash of the previous block |
| `timestamp` | Block time, converted from the RPC millisecond epoch |
| `timezone` | Always `UTC` |
| `total_transactions` | Total number of transactions in the block (sum of the three counts below) |
| `public_transactions` | Count of `Public` transactions |
| `privacy_preserving_transactions` | Count of `PrivacyPreserving` transactions |
| `program_deployment_transactions` | Count of `ProgramDeployment` transactions |
| `status` | The block's `bedrock_status` as reported by the indexer. Should always be `Finalized`, since the connector only syncs up to the last finalized block (`getLastFinalizedBlockId`). Be careful when processing the latest block - if it is not yet finalized this value will differ. |
| `rpc_method` | RPC method used to fetch the block. Kept for debugging purposes. |

### `public_transactions`

One record per `Public` transaction in a block:

| Field | Description |
| --- | --- |
| `hash` | Transaction hash (primary key) |
| `program_id` | `program_id` from the transaction message |
| `accounts` | Number of `account_ids` on the transaction |
| `account_ids` | Account IDs referenced by the transaction |
| `block_id` | ID of the containing block |
| `block_hash` | Hash of the containing block |
| `block_timestamp` | Timestamp of the containing block |
| `timezone` | Always `UTC` |

### `privacy_preserving_transactions`

One record per `PrivacyPreserving` transaction in a block:

| Field | Description |
| --- | --- |
| `hash` | Transaction hash |
| `public_actions` | `public_actions` array from the transaction message (list of objects) |
| `block_id` | ID of the containing block |
| `block_hash` | Hash of the containing block |
| `block_timestamp` | Timestamp of the containing block |
| `timezone` | Always `UTC` |

### `program_deployment_transactions`

One record per `ProgramDeployment` transaction in a block:

| Field | Description |
| --- | --- |
| `hash` | Transaction hash |
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
