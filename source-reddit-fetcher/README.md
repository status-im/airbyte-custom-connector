# Reddit Source


## Usage

This connector fetches information from one or multiple selected subreddits.

### Configuration

The connector takes the following input:

- `client_id` - Reddit's client ID for your account
- `client_secret` - Reddit's client secret for your account
- `username` - your Reddit username that has been used to generate the `client_id` and `client_secret`
- `subreddits` - array of subreddit names to monitor (e.g., ["privacy", "technology", "programming"])
- `days` - used to calculate the stop date. Before that date votes and comments will not be monitored. Calculation: $stop\ date = today - days$

#### Configuration Example

```json
{
    "client_id": "your_reddit_client_id",
    "client_secret": "your_reddit_client_secret", 
    "username": "your_reddit_username",
    "subreddits": ["privacy", "technology", "programming"],
    "days": 31
}
```

### Output

The connector will return the following streams (one pair per subreddit):

* **Posts streams**: `posts_{subreddit_name}` - Contains posts from each subreddit ([schema](./source_reddit_fetcher/schemas/posts.json))  
* **Comments streams**: `comments_{subreddit_name}` - Contains comments from posts in each subreddit ([schema](./source_reddit_fetcher/schemas/comments.json))



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