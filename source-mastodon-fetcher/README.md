# Mastodon Source

## Usage

This connector fetches information from a given [Mastodon servers](https://joinmastodon.org/servers). Once the data is uploaded to a database, a lot of useful account and post information can be extracted. To learn more about the REST API limit please go through the [documentation](https://docs.joinmastodon.org/api/rate-limits/). 

### Configuration

The connector takes the following input:

- `access_token` - the access token field from the Mastodon application.
- `url_base` - the base URL of the Mastodon server.
- `tags` - the `#` that will be followed. A stream is created per `#`.
- `accounts` - the account names that will be followed for posts. A stream is created per account username.
- `days` - how many days in the past to look for posts. By default -1 will try and get all of the files.

### Output

The connector will return the following:

* [AccountFeed](./source_mastodon_fetcher/schemas/account_feed.json)
* [TagFeed](./source_mastodon_fetcher/schemas/tag_feed.json)

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
docker build -t airbyte/source-mastodon-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/source-mastodon-fetcher:dev spec
```

#### Run

Then run any of the connector commands as follows:

#### Linux / MAC OS

```
docker run --rm airbyte/source-mastodon-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-mastodon-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-mastodon-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-mastodon-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json  
```

### Windows

```
docker run --rm airbyte/source-mastodon-fetcher:dev spec
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-mastodon-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-mastodon-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-mastodon-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
