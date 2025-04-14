# Apple Store Source

This is the repository for the Apple Store source connector, written in Python.
For information the Apple Store connect API [see here](https://developer.apple.com/documentation/AppStoreConnectAPI/downloading-analytics-reports).

## App Store Connect API Reports

This connector extracts reports from the App Store Connect API.

### Report Structure

- Each report type has a unique ID which can be found in `reports.csv`
- Reports are generated daily by Apple and typically contain data from the current day and previous 3 days,due to this rolling window, reports from consecutive days may contain duplicate data so in these connector for each report we only load data that corresonds to the day before the report was generated.

### Sync Behavior

- Each sync processes reports between `start_date` and `end_date`
  - `end_date` defaults to today 
  - `start_date` defaults to 4 days ago
- The connector downloads fresh copies of reports on each sync
- If a sync is run multiple times in the same day, newer downloads will replace older ones

### Report Processing

- For each report type, the connector finds available instances (a report instance represents a specific date)
- The processing date in the report metadata is the date when Apple generated the report
- Records are filtered to include only data matching the expected date (1 day before the day of the report)

### Available Reports

#### App Install Performance
- Documents app installation performance metrics
- Documentation: https://developer.apple.com/documentation/analytics-reports/app-installs-performance
- ID: r5-1032fee7-dfb3-4a4a-b24d-e603c95f5b09

#### Additional Reports
- App Downloads Detailed
- App Installation and Deletion Detailed
- App Sessions Detailed
- App Discovery and Engagement Detailed

### Technical Notes

- The connection may take several minutes to complete due to the API response times
- A deduplication mechanism is implemented in the `read_records` function to handle overlapping data

## Overview

This connector extracts extract data from diferent apple store reports using the App store connect API.

### Output schema

This connector outputs the following streams that corresponds each to a specific report:

1. **App Installs Performance** 
2. **App Downloads Detailed** 
3. **App Installation and Deletion Detailed**
4. **App Sessions Detailed**
5. **App Discovery and Engagement Detailed**

### Configuration

The connector requires the following configuration parameters:

```yaml
    key_id:
      description: Your App Store Connect API Key ID.
    issuer_id:
      description: Your App Store Connect API Issuer ID (found in the API Keys section of App Store Connect).
    private_key:
      description: The private key content for the App Store Connect API. Include the entire key, including the BEGIN and END lines.
    start_date (optional):
      description: The date to start syncing data from, in YYYY-MM-DD format. If not provided, defaults to 3 days ago. The oldest date available is the 2025-04-02 (because no reports request were made before this date).
      examples:
        - "2023-01-01"
    end_date (optional):
      description: The date to sync data until, in YYYY-MM-DD format. If not provided, defaults to the current date.
      examples:
        - "2023-04-30"
    report_ids (optional):
      description: Custom report IDs to use for fetching data. If not provided, default IDs will be used.

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
docker build -t airbyte/source-app-store:dev .
# Running the spec command against your patched connector
docker run airbyte/source-app-store:dev spec
```

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/source-app-store:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-app-store:dev check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/source-app-store:dev discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files -v $(pwd)/sample_files:/sample_files airbyte/source-app-sore:dev read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```

## Notes

- reports.csv is the list of all the reports that exist with theyr name, category and id 
- be aware that the scripts can take some time to execute (some minutes)

* App Install Performance
a new report is generated every day, in this report we have data from the current day and the previous 3 days. so we can have duplicates in the data for example between 2025-04-01 and 2025-04-02 reports.
This is why we implemented a deduplication mechanism in the read_records function.

Doc about the report: https://developer.apple.com/documentation/analytics-reports/app-installs-performance

if the sync is tun twice the same day, the second run will override the first one. (ir ancient downloads will be replaced by the new ones)

Process every report with date between start_date and end_date (end_date defaults to today start_date defaults to 4 days ago) porcessing date is the date of the report.

Each sync will download fresh copies of the reports
Old files will be overwritten with new data

Reports ids can be found in the reports.csv file, this file is generated after making a request report api call with adming credentials.

for each report an instance is equivalent to a report's date so there is one instance per report per day