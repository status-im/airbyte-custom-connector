# Wallet Fetcher Source

This is the repository for the Wallet Fetcher source connector, written in Python.

## Usage

This connector fetch wallet balance on different blockchain.

The Supported chains:
* Bitcoin
* Ethereum

### Configuration

The connector takes the following input:

```yaml
wallets:
  address:
    title: Address
    type: string
    description: Address of the wallet
  name:
    title: Name
    type: string
    description: Name of the wallet
  blockchain:
    title: blockchain
    description: List of blockchain concerning the wallets
    type: array
    items:
      type: string
      enum:
        - BTC
        - ETH
  tags:
    title: tags
    type: string
    description: List of tags linked to the wallet
```



### Output

This connector will return a list of coin for each chain with the following models

* `wallet_name`: Name of the wallet
* `name`: Name of the coin.
* `symbol`: Symbol of the coin.
* `description`: Description of the ERC-20 Token.
* `address`: Address of the Smart contract for ERC-20 Token.
* `chain`: Name of the blockchain.
* `balance`: Number of token owned.
* `decimal`: Number of decimal for the token.
* `tags`: Tags associated with the wallet owning the token.


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
python main.py check --config secrets/config.json
python main.py discover --config secrets/config.json
python main.py read --config secrets/config.json --catalog integration_tests/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t airbyte/source-wallet-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/source-wallet-fetcher:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-wallet-fetcher:dev spec
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-wallet-fetcher:dev check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets airbyte/source-wallet-fetcher:dev discover --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets -v $(pwd)/integration_tests:/integration_tests airbyte/source-wallet-fetcher:dev read --config /secrets/config.json --catalog /integration_tests/configured_catalog.json
```

