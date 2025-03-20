# Status Network Source

This is the repository for fetching Status Network API information, written in Python.

## Usage

This connector fetch information from the status Network Scanner
- Sepolia: https://sepoliascan.status.network/

### Configuration

The connector takes the following input:

```yaml
```

### Output

The connector will return the following:
* [Stats](./source-blockchain-explorer/schemas/stats.json)  
* [Block](./source-blockchain-explorer/schemas/blocks.json)  
* [Transaction](./source-blockchain-explorer/schemas/transactions.json)

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
docker build -t airbyte/source-blockchain-explorer:dev .
# Running the spec command against your patched connector
docker run airbyte/source-blockchain-explorer:dev spec
````

#### Run
Then run any of the connector commands as follows:

#### Linux / MAC OS
```
docker run --rm airbyte/source-blockchain-explorer:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-blockchain-explorer:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-blockchain-explorer:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-blockchain-explorer:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```

### Windows
```
docker run --rm airbyte/source-blockchain-explorer:dev spec
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-blockchain-explorer:dev check --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-blockchain-explorer:dev discover --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-blockchain-explorer:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```