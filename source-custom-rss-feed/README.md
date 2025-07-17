# Custom RSS Feed Connector

A custom Airbyte connector that extracts articles from RSS feeds with support for incremental sync and date filtering.

## Overview

This connector allows you to sync article data from one or multiple RSS feeds into your data warehouse. It supports incremental synchronization using article publication dates and provides flexible filtering options.

## Features

✅ **Multiple RSS Feeds** - Extract from multiple RSS feed URLs simultaneously  
✅ **Incremental Sync** - Only fetch new articles since the last sync  
✅ **Date Filtering** - Optional start_time parameter to filter articles by publication date  
✅ **Rich Data Extraction** - Extracts comprehensive article metadata  
✅ **Error Resilience** - Graceful handling of malformed feeds  
✅ **Source Tracking** - Tracks which feed each article originated from  

## Installation

### Docker Image
```
harbor.status.im/bi/airbyte/source-custom-rss-feed:v2.0.1
```

### Local Development
```bash
git clone <repository-url>
cd source-custom-rss-feed
pip install -r requirements.txt
pip install -e .
```

## Configuration

### Required Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `rss_urls` | Array of URLs | List of RSS feed URLs to extract data from | `["https://netblocks.org/feed"]` |

### Optional Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `start_time` | ISO 8601 DateTime | Only extract articles published after this date. Defaults to one week ago if not provided. | `"2024-01-01T00:00:00Z"` |

### Configuration Example

```json
{
  "rss_urls": [
    "https://netblocks.org/feed",
    "https://feeds.feedburner.com/oreilly/radar"
  ],
  "start_time": "2024-01-01T00:00:00Z"
}
```

## Data Schema

The connector extracts articles with the following fields:

| Field | Type | Description | Primary Key | Cursor Field |
|-------|------|-------------|-------------|--------------|
| `guid` | String | Globally unique identifier for the article | ✅ | |
| `title` | String | Article title | | |
| `author` | String | Article author | | |
| `content` | String | Full article content (HTML) | | |
| `description` | String | Article summary/description | | |
| `link` | String | URL to the full article | | |
| `published` | DateTime | Publication date and time | | ✅ |
| `_source_feed_url` | String | Source RSS feed URL (automatically added) | | |

## Sync Modes

### Full Refresh
Extracts all articles from the RSS feeds on each sync.

### Incremental Sync
Only extracts articles that were published after the last successful sync. Uses the `published` field as the cursor.

## Usage Examples

### Basic Usage
Extract all articles from a single RSS feed:
```json
{
  "rss_urls": ["https://netblocks.org/feed"]
}
```

### Multiple Feeds
Extract from multiple RSS feeds:
```json
{
  "rss_urls": [
    "https://netblocks.org/feed",
    "https://feeds.feedburner.com/oreilly/radar",
    "https://example.com/news.rss"
  ]
}
```

### Date Filtering
Only extract articles published after January 1, 2024:
```json
{
  "rss_urls": ["https://netblocks.org/feed"],
  "start_time": "2024-01-01T00:00:00Z"
}
```

## Testing

### Local Testing

1. **Test connection:**
```bash
python main.py check --config sample_files/config-example.json
```

2. **Discover schema:**
```bash
python main.py discover --config sample_files/config-example.json
```

3. **Test data extraction:**
```bash
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```

### Docker Testing

1. **Build image:**
```bash
docker build -t source-custom-rss-feed .
```

2. **Test connection:**
```bash
docker run --rm -v $(pwd)/sample_files:/tmp/sample_files \
  source-custom-rss-feed check --config /tmp/sample_files/config-example.json
```

## Development

### Prerequisites
- Python 3.9+
- Docker
- Airbyte CDK

## Limitations

- RSS feeds must be publicly accessible (no authentication support)
- Very large RSS feeds may have memory limitations
- Some RSS feeds may have rate limiting (respected automatically)
- Date parsing depends on RSS feed providing publication dates
