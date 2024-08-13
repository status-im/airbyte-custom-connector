# BambooHR Fetcher Source

This is the repository for fetching data from BambooHR API, written in Python.

## Usage

This connector fetches employees data in BambooHR`.

### Configuration

The connector takes the following input:

```yaml
token: 'Authentication Token'
```

### Output

The connector will return the following:
- `employees`: List employees.
- `employees_details` : Detail of each employees (team, gh name, ...)

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
docker run --rm harbor.status.im/status-im/airbyte/source-custom-bamboohr-hr:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/status-im/custom-banboo-hr:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/status-im/custom-banboo-hr:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files harbor.status.im/status-im/custom-banboo-hr:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
