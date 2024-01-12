# Crypto Market Extractor Source

This is the repository for the Crypto Market Extractor source connector, written in Python.

## Usage

This connector fetch coins value on API.

The Supported API:
* CoinGecko

### Configuration

The connector takes the following input:

```yaml
coins:
  type: array
  name: coins
  description: List of coin to fetch the price. List available at coingecko api under `/coins/list`
  items:
    type: string
```

### Output

This connector will return a list of coin for each chain with the following models

* `name`: Name of the coin.
* `price`: Price in USD of the coin
* `date`: Date of the sync.

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
python main.py check --config sample_files/coin_list.json
python main.py discover --config sample_files/coin_list.json
python main.py read --config sample_files/coin_list.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/crypto-market-extractor:dev .
# Running the spec command against your patched connector
docker run airbyte/crypto-market-extractor:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/crypto-market-extractor:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/crypto-market-extractor:dev check --config /sample_files/coin_list.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/crypto-market-extractor:dev discover --config /sample_files/coin_list.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/crypto-market-extractor:dev read --config /sample_files/coin_list.json --catalog /sample_files/configured_catalog.json
