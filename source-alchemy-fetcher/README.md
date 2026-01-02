# Mastodon Source

## Usage

This connector fetches historical prices from [Alchemy](https://www.alchemy.com/).

### Configuration

The connector takes the following input:

- `api_key` - the [Alchemy](https://www.alchemy.com/) API key.
- `tokens` - there are two different `payload` implementations:
  - [Symbol](https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-historical-token-prices#request.body.PricesApiEndpointsGetHistoricalTokenPricesRequest0) - requires `symbol` and `interval`.
  - [Address](https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-historical-token-prices#request.body.PricesApiEndpointsGetHistoricalTokenPricesRequest1) - requires `network`, `address`, `symbol` and `interval`.

#### Token Setup

Symbol example:

```json
{
    "symbol": "BTC",
    "interval": "5m"
}
```

Address example:

```json
{
    "network": "opt-mainnet",
    "address": "0x4200000000000000000000000000000000000042",
    "symbol": "OP",
    "interval": "1d"
}
```

**Notes**:

- `interval` - can be `5m`, `1h`, `1d`. [Range calculations](https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-historical-token-prices#request.body.PricesApiEndpointsGetHistoricalTokenPricesRequest0.interval) are automatically done in the stream slices.
- `start_date` and `end_date` can be used for backfills. Once a backfill is completed, they can be left blank. The prices for the given `interval` will be fetched for the previous day automatically.

### Output

The connector will return the following:

* [HistoricalRates](./source_alchemy_fetcher/schemas/historical_rates.json)

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
docker build -t airbyte/source-alchemy-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/source-alchemy-fetcher:dev spec
```

#### Run

Then run any of the connector commands as follows:

#### Linux / MAC OS

```
docker run --rm airbyte/source-alchemy-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-alchemy-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-alchemy-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-alchemy-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json  
```

#### Windows

```
docker run --rm airbyte/source-alchemy-fetcher:dev spec
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-alchemy-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-alchemy-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-alchemy-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```