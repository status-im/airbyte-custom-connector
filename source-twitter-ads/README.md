# Twitter Fetcher Source

This is the repository for fetching Twitter Ads Analytics information, written in Python.

## Usage

This connector fetch information from Twitter Ads based on their API: https://docs.x.com/x-ads-api/
### Configuration

The connector takes the following input:

```yaml
twitter:
  credientials:
    consumer_key: ''
    consumer_secret: ''
    access_key: ''
    access_secret: ''
  account_ids: "List of Twitter account IDs"
  start_time: 'AAAA-MM-DDTHH:mm:SSZ" # Start of the period of tweets sync
```

To obtain the `account_id`, run the following command:
```bash
curl -X GET "https://api.x.com/2/users/me" \
    -H "Authorization: Bearer $access_token"
```

### Output

The connector will return the following:


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
docker build -t airbyte/source-twitter-ads:dev .
# Running the spec command against your patched connector
docker run airbyte/source-twitter-ads:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-twitter-ads:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-twitter-ads:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-twitter-ads:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-twitter-ads:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
