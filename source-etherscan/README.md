# Etherscan Source

## Usage

This Airbyte connector fetch information from Etherscan API.

It get:
* Internal balance
* Internal transaction
* Specific ERC20 balance
* Specific ERC20 transaction

### Configuration

The connector takes the following input:

```yaml
api_key: 'example-key'
chain_id: 1
wallets:
  - name: 'example'
    address: '0x0000000000000000000000000000000000000000'
    tag: 'Test'
tokens:
  - name: 'SNT'
    address: '0x744d70fdbe2ba4cf95131626614a1763df805b9e'
```

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
