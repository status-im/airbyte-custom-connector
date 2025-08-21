# Discourse Fetcher Source

This is the repository for fetching data from Discourse forum, written in Python.

## Usage

This connector fecth user and post data from a discourse forum instance.

### Configuration

The connector takes the following input:

```yaml
- api-key
- api-username
- url
```

### Output

The connector will return the following:
- `posts`: List of post on the discourse instance.
- `users`: List of user on the discourse instance.

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
docker build -t airbyte/source-discourse-fetcher:dev .
# Running the spec command against your patched connector
docker run airbyte/source-discourse-fetcher:dev spec
```

#### Run
Then run any of the connector commands as follows:

```
docker run --rm airbyte/source-discourse-fetcher:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-discourse-fetcher:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-discourse-fetcher:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-discourse-fetcher:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
