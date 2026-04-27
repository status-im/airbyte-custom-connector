# Logos Blockchain Source
    
This connector is experimental and will change in future, when Logos moves to mainnet.

## Usage

This Airbyte connector extracts data from a Logos Blockchain Node.

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
