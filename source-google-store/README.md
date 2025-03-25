# Google Play Store Reviews Source

This is the repository for the Google Play Store Reviews source connector, written in Python.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/google-store).

## Overview

This connector extracts reviews data from the Google Play Store for your Android application. It uses the Google Play Store data export functionality via Google Cloud Storage to access the reviews data.

### Output schema

This connector outputs the following streams:


* [Reviews](./source-google-store/source_google_store/schemas/reviews.json) - User reviews for your Android app
* [Installs Overview](./source-google-store/source_google_store/schemas/installs_overview.json) - Installs overview data for your Android app
* [Installs by Android Version](./source-google-store/source_google_store/schemas/installs_android_version.json) - Installs data grouped by Android version
* [Installs by App Version](./source-google-store/source_google_store/schemas/installs_app_version.json) - Installs data grouped by app version
* [Installs by Country](./source-google-store/source_google_store/schemas/installs_country.json) - Installs data grouped by country
* [Crashes Overview](./source-google-store/source_google_store/schemas/crashes_overview.json) - Crashes overview data for your Android app
* [Crashes by Android Version](./source-google-store/source_google_store/schemas/crashes_android_version.json) - Crashes data grouped by Android version
* [Crashes by App Version](./source-google-store/source_google_store/schemas/crashes_app_version.json) - Crashes data grouped by app version
* [Ratings Overview](./source-google-store/source_google_store/schemas/ratings_overview.json) - Ratings overview data for your Android app
* [Ratings by Android Version](./source-google-store/source_google_store/schemas/rating_android_version.json) - Ratings data grouped by Android version
* [Ratings by App Version](./source-google-store/source_google_store/schemas/rating_app_version.json) - Ratings data grouped by app version
* [Ratings by Country](./source-google-store/source_google_store/schemas/rating_country.json) - Ratings data grouped by country

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
docker build -t airbyte/source-google-store:dev .
# Running the spec command against your patched connector
docker run airbyte/source-google-store:dev spec
````

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-google-store:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-google-store:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-google-store:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-google-store:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
