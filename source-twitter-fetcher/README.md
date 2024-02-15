# Twitter Fetcher Source

This is the repository for fetching Twitter information, written in Python.

## Todos

* [ ] Implements first version based on original script
  * [ ] Fetch the Data users of each count.
  * [ ] Fetch Tweets details 
* [ ] Improve version:
  * limit the data fetching based on input date


## Usage

This connector fetch information from Twitter based on their API: https://developer.twitter.com/en/docs/twitter-api

### Configuration

The connector takes the following input:

```yaml
twitter:
  - API-KEY
  - Account List
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
docker build -t airbyte/twitter-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/twitter-fetcher:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/twitter-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/twitter-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
