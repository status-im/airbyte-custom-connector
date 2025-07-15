# Reddit Source


## Usage

This connector fetch information from a selected subreddit.

### Configuration

The connector takes the following input:

- `client_id` - Reddit's client ID for your account
- `client_secret` - Reddit's client secret for your account
- `username` - your Reddit username that has been used to generate the `client_id` and `client_secret`
- `days` - used to calculate the stop date. Before that date votes and comments will not be monitored. Calculation: $stop\ date = today - days$

### Output

The connector will return the following:

* [Posts](./source-reddit-fetcher/schemas/posts.json)  
* [PostVotes](./source-reddit-fetcher/schemas/posts_votes.json)
* [Comments](./source-reddit-fetcher/schemas/comments.json)
* [CommentsVotes](./source-reddit-fetcher/schemas/comments_votes.json)

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
docker build -t airbyte/source-reddit-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/source-reddit-fetcher:dev spec
```

#### Run

Then run any of the connector commands as follows:

#### Linux / MAC OS

```
docker run --rm airbyte/source-reddit-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-reddit-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-reddit-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-reddit-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json  
```

### Windows

```
docker run --rm airbyte/source-reddit-fetcher:dev spec
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-reddit-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-reddit-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v "$PWD\sample_files:/sample_files" airbyte/source-reddit-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```