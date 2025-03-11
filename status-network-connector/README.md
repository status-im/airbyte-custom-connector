# Twitter Fetcher Source

This is the repository for fetching Twitter information, written in Python.

## Usage

This connector fetch information from the status Network Scanner
- Sepolia: https://sepoliascan.status.network/

### Configuration

The connector takes the following input:

```yaml
```

### Output

The connector will return the following:
* [Stats](./source_status_network/schemas/stats.json)
* [Block](./source_status_network/schemas/blocks.json)
* [Transaction](./source_status_network/schemas/transactions.json)

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
docker build -t airbyte/source-status-network-connector:dev .
# Running the spec command against your patched connector
docker run airbyte/source-status-network-connector:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-status-network-connector:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-status-network-connector:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-status-network-connector:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-status-network-connector:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
