# Alchemy Token Price Fetcher

Airbyte connector to fetch cryptocurrency token prices from the [Alchemy API](https://docs.alchemy.com/reference/token-api-quickstart).

## Features

- Fetch **current prices** for multiple tokens in a single request
- Fetch **historical prices** for a specific timestamp
- Incremental sync with append mode (builds price history over time)

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `api_key` | string | Yes | Your Alchemy API key |
| `symbols` | array | Yes | List of token symbols (e.g., ETH, BTC, USDT) |
| `historical_fetch` | boolean | No | Enable historical price fetching (default: false) |
| `historical_timestamp` | string | No | ISO 8601 timestamp for historical prices (e.g., `2024-01-01T00:00:00Z`) |

### Example: Current Prices

```json
{
  "api_key": "your_alchemy_api_key",
  "symbols": ["ETH", "BTC", "USDT", "SNT"]
}
```

### Example: Historical Prices

```json
{
  "api_key": "your_alchemy_api_key",
  "symbols": ["ETH", "BTC"],
  "historical_fetch": true,
  "historical_timestamp": "2024-01-01T00:00:00Z"
}
```

## Output

| Field | Description |
|-------|-------------|
| `symbol` | Token symbol (e.g., ETH) |
| `price_usd` | Price in USD |
| `price_last_updated_at` | When the price was last updated (cursor field) |
| `currency` | Currency of the price (usd) |

## Local Development

### Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -e .
```

### Run Commands

```bash
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Docker

```bash
docker build -t airbyte/source-alchemy-fetcher:dev .
docker run --rm airbyte/source-alchemy-fetcher:dev spec
```
