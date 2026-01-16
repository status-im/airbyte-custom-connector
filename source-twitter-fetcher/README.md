# Twitter Fetcher Source

This is the repository for fetching Twitter information, written in Python.

## Usage

This connector fetch information from Twitter based on their API: https://developer.twitter.com/en/docs/twitter-api

### Configuration

The connector takes the following input:

```yaml
twitter:
  credientials:
    client_id: "Id from the Twitter Developer Account"
    client_secret: "Secret from the Twitter Developer Account"
    access_token: "Token generated from the generated Twitter account"
    refresh_token: "Refresh token obtain from the Twitter Account"
    bearer_token: "Bearer Token form Twitter Dev Portal"
    token_expiry_date: "Expiry date off the Token Access"
  account_id: "Id of the Twitter account"
  start_time: "AAAA-MM-DDTHH:mm:SSZ" # Start of the period of tweets sync
  tags: ["List", "Hashtag", "mentions", "keyword"]
  tags_frequent_extractions: False
  space_ids: ["id of space to monitores"]
  space_account: ["Id of account making the space to monitores"]
```

To obtain the `account_id`, run the following command:
```bash
curl -X GET "https://api.x.com/2/users/me" \
    -H "Authorization: Bearer $access_token"
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
```
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

```bash
docker build -t harbor.status.im/bi/airbyte/source-twitter-fetcher:dev .
# Running the spec command against your patched connector
docker run harbor.status.im/bi/airbyte/source-twitter-fetcher:dev spec
````

#### Run
Then run any of the connector commands as follows:

```bash
docker run --rm harbor.status.im/bi/airbyte/source-twitter-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-twitter-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-twitter-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-twitter-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
