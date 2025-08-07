# Bluesky Fetcher

This is the setup for the Bluesky Fetcher source connector which extracts posts from Bluesky using the AT Protocol API.

## Features

- Authenticate with Bluesky using handle/email and app password
- Search for posts using various query formats (hashtags, from specific users, keywords)
- Extract detailed post data including author information, content, engagement metrics
- Support for incremental synchronization
- Configurable post limits and time-based filtering

## Configuration

The connector requires the following configuration:

### Required fields:
- `identifier`: Your Bluesky handle (e.g., "username.bsky.social") or email address
- `password`: Your Bluesky app password (not your main account password)

### Optional fields:
- `service_url`: Bluesky service URL (defaults to "https://bsky.social")
- `search_query`: Search query for posts (e.g., "#ai", "from:username.bsky.social", "keyword")
- `limit`: Maximum number of posts to retrieve per request (1-100, defaults to 25)
- `start_time`: ISO timestamp to filter posts from (defaults to one week ago)

## Search Query Examples

- `#ai` - Search for posts with hashtag #ai
- `from:bsky.app` - Search for posts from a specific user
- `artificial intelligence` - Search for posts containing keywords
- Leave empty to get timeline posts

## Data Schema

The connector extracts the following data for each post:

- `uri`: Unique AT Protocol URI for the post
- `cid`: Content identifier
- `author`: Author information (handle, display name, avatar, etc.)
- `record`: Post content (text, creation time, languages)
- `embed`: Embedded content (images, links, etc.)
- Engagement metrics: reply_count, repost_count, like_count
- `indexed_at`: When the post was indexed
- `labels`: Content moderation labels

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python main.py spec
python main.py check --config sample_files/config-example.json
python main.py discover --config sample_files/config-example.json
python main.py read --config sample_files/config-example.json --catalog sample_files/configured_catalog.json
```
