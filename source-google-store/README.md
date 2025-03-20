# Google Play Store Reviews Source

This is the repository for the Google Play Store Reviews source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/google-store).

## Overview

This connector extracts reviews data from the Google Play Store for your Android application. It uses the Google Play Store data export functionality via Google Cloud Storage to access the reviews data.

### Output schema

This connector outputs the following stream:

1. **Reviews** - User reviews for your Android app

### Features

| Feature | Supported? |
| --- | --- |
| Full Refresh Sync | Yes |
| Incremental Sync | No |
| Namespaces | No |

### Configuration

The connector takes the following input:

```yaml
client_email: 'service account'
credentials_json: 'copy paste the json credentials'
package_name : 'im.status.ethereum app reference'
bucket : 'id that can be found on the google store'
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
