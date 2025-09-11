# Luma Fetcher Source

Airbyte connector for fetching events and guest data from the Luma API.

## Overview

This connector extracts data from Luma's public API to sync event information and associated guest details.

### Output Streams

1. **luma_events_stream** - Calendar events with details like name, description, location, and timing
2. **luma_guests_stream** - Guest information for each event including registration details and check-in status

### Features

| Feature | Supported? |
| --- | --- |
| Full Refresh Sync | Yes |
| Incremental Sync | Yes (guests stream) |
| Rate Limiting | Yes |
| Composite Primary Keys | Yes |

### Configuration

```yaml
api_key:
  type: string
  description: Your Luma API key
  required: true
  secret: true
```



## Local Development

### Python Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Test Commands
```bash
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Docker Setup
```bash
docker build -t harbor.status.im/bi/airbyte/source-luma-fetcher:v2.0.0 .
```

### Docker Commands
```bash
docker run --rm harbor.status.im/bi/airbyte/source-luma-fetcher:v2.0.0 spec
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-luma-fetcher:v2.0.0 check --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-luma-fetcher:v2.0.0 discover --config /sample_files/config-example.json
docker run --rm -v $(pwd)/sample_files:/sample_files harbor.status.im/bi/airbyte/source-luma-fetcher:v2.0.0 read --config /sample_files/config-example.json --catalog /sample_files/configured_catalog.json
```
