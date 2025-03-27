# Google Play Store Reviews Source

This is the repository for the Google Play Store Reviews source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/google-store).

## Overview

This connector extracts reviews data from the Google Play Store for your Android application. It uses the Google Play Store data export functionality via Google Cloud Storage to access the reviews data.

### Output schema

This connector outputs the following stream:

1. **Reviews** - User reviews for your Android app
2. **Installs** 
3. **Crashes**
4. **Ratings**

### Features

| Feature | Supported? |
| --- | --- |
| Full Refresh Sync | Yes |
| Incremental Sync | No |
| Namespaces | No |

servers instances.

### Configuration

The connector requires the following configuration parameters:

```yaml
client_email:
  type: string
  description: The email address of the service account used to access the Google Play Store data
  required: true
  secret: true

credentials_json:
  type: string
  description: The JSON key file obtained when creating the service account
  required: true
  secret: true

package_name:
  type: string
  description: The package name of the Android app (e.g., im.status.ethereum)
  required: true

bucket:
  type: string
  description: The Google Cloud Storage bucket containing the Play Store reports
  required: true
  secret: true

start_date:
  type: string
  description: Start date to fetch reports from, in YYYYMM format (e.g., 202301)
  required: false
  pattern: YYYYMM
  example: "202401"
  default: Previous month
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
docker build -t airbyte/source-google-store:dev .
# Running the spec command against your patched connector
docker run airbyte/source-google-store:dev spec
```

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-google-store:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-google-store:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-google-store:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-google-sore:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```