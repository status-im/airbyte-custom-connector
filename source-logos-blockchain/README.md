# Logos Blockchain Source
    
This connector is experimental and will change in future, when Logos moves to mainnet.

## Usage

This Airbyte connector extracts data from a **[Logos Blockchain Node 0.2](https://github.com/logos-blockchain/logos-blockchain/releases/tag/0.2.0)**.

## Local development

To run the Airbyte connector locally:

1. Make sure [Logos Blockchain Node 0.2.0](https://github.com/logos-blockchain/logos-blockchain/releases/tag/0.2.0) is set up.

2. By default, the Logos Blockchain node does not expose its IP address externally. For example, if the blockchain is running on a Raspberry Pi and the Airbyte connector is running on a different device, you'll need to establish an `ssh` tunnel: `ssh -N -L 8080:localhost:8080 your-username@ip-address`.

3. Update the [config](./sample_files/config-example.json) `url` field to `http://host.docker.internal:8080/`.

### Prerequisites

From this connector directory, create a virtual environment:
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Locally running the connector
```bash
poetry run source-logos-blockchain spec
poetry run source-logos-blockchain check --config secrets/config.json
poetry run source-logos-blockchain discover --config secrets/config.json
poetry run source-logos-blockchain read --config secrets/config.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/source-logos-blockchain:dev .
# Running the spec command against your patched connector
docker run airbyte/source-logos-blockchain:dev spec
```

#### Run
Then run any of the connector commands as follows:
```bash
docker run --rm airbyte/source-logos-blockchain:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-logos-blockchain:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-logos-blockchain:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/source-logos-blockchain:dev read --config /secrets/config.json --catalog /sample_files/configured_catalog.json
```
