# Logos Execution Zone Source
    
This connector is experimental and will change in future, when Logos moves to mainnet.

## Usage

This Airbyte connector extracts data from a **[Logos Execution Zone (LEZ)](https://github.com/logos-blockchain/logos-execution-zone/tree/main)**.

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
