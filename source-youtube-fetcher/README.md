# YouTube Fetcher Source Connector

An Airbyte source connector for extracting YouTube channel and video data using the YouTube Data v3 API.

## Overview

The YouTube Fetcher connector enables you to sync YouTube channel statistics and video data into your data warehouse or analytics platform through Airbyte. This connector fetches comprehensive data about YouTube channels and their videos, including statistics, metadata, and engagement metrics.

## Supported Streams

| Stream Name | Description | Sync Mode | Primary Key |
|-------------|-------------|-----------|-------------|
| `channel` | Channel information, statistics, and metadata | Full Refresh | `channel_id` |
| `videos` | Video details, statistics, and metadata from the channel | Full Refresh, Incremental | `video_id` |

### Channel Stream

The channel stream provides comprehensive information about the YouTube channel:

- **Basic Information**: Title, description, custom URL, publication date
- **Statistics**: Subscriber count, video count, total view count
- **Metadata**: Country, default language, thumbnails
- **Playlists**: Upload, likes, favorites playlist IDs
- **Branding**: Keywords, featured channels, banner image
- **Status**: Privacy status, linked status, upload capabilities
- **Topics**: Associated topic categories and IDs

### Videos Stream

The videos stream provides detailed information about all videos from the channel:

- **Basic Information**: Title, description, publication date, thumbnails
- **Statistics**: View count, like count, comment count, favorite count
- **Content Details**: Duration (ISO and seconds), definition, captions, licensing
- **Status**: Privacy status, embeddable, public stats viewable
- **Metadata**: Tags, category, language, live broadcast content
- **Channel Context**: Channel title, subscriber count for analysis

## Prerequisites

1. **YouTube Data API v3 Key**: 
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the YouTube Data API v3
   - Create credentials (API key)
   - Copy the API key for connector configuration

2. **Channel Access**: Ensure the target YouTube channel is public and accessible

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `api_key` | string | Yes | Your YouTube Data API v3 key |
| `channel_identifier` | string | Yes | Channel identifier (handle, username, or channel ID) |
| `max_results` | string | No | Maximum videos to fetch ("all" or number, default: "all") |
| `include_comments_count` | boolean | No | Fetch detailed comment counts (slower, default: false) |
| `start_date` | string | No | Start date for incremental syncs (YYYY-MM-DD) |
| `fetch_channel_analytics` | boolean | No | Include channel analytics (default: true) |

### Channel Identifier Formats

The connector supports multiple channel identifier formats:

- **Handle**: `@channelname` (recommended for newer channels)
- **Channel ID**: `UCBa659QWEk1AI4Tg--mrJ2A` (starts with UC)
- **Username**: `channelname` (legacy format)

## Sync Modes

### Full Refresh

Both streams support full refresh mode, which replaces all data in the destination with the latest data from YouTube.

### Incremental Sync

The `videos` stream supports incremental sync using the `published_at` field as the cursor. This allows you to:

- Sync only new videos published since the last sync
- Reduce API quota usage and sync time
- Set a `start_date` to begin incremental syncs from a specific date

## Performance and API Limits

### YouTube API Quotas

The YouTube Data API v3 has the following limits:
- **Daily quota**: 10,000 units per day (default)
- **Requests per 100 seconds**: 100 per user

### Quota Costs

| Operation | Cost (Units) |
|-----------|--------------|
| Channel information | 1 |
| Video list (50 videos) | 1 |
| Video details (50 videos) | 1 |
| Comment count per video | 1 |

### Optimization Tips

1. **Limit video count**: Use `max_results` for testing or specific use cases
2. **Disable comment fetching**: Keep `include_comments_count` as false unless needed
3. **Use incremental sync**: Configure incremental mode for the videos stream
4. **Monitor quota usage**: Check your API quota in Google Cloud Console

## Setup Instructions

### In Airbyte OSS

1. Navigate to **Sources** in your Airbyte instance
2. Click **+ New Source**
3. Search for and select **YouTube Fetcher**
4. Configure the connection with your API key and channel identifier
5. Test the connection
6. Configure streams and sync settings
7. Set up destination and run your first sync

### In Airbyte Cloud

1. Go to your Airbyte Cloud dashboard
2. Click **Sources** → **+ New Source**
3. Select **YouTube Fetcher** from the connector catalog
4. Follow the configuration steps above

## Example Configurations

### Basic Configuration

```json
{
  "api_key": "your_youtube_api_key",
  "channel_identifier": "@TechChannel",
  "max_results": "all",
  "include_comments_count": false,
  "fetch_channel_analytics": true
}
```

### Limited Video Sync

```json
{
  "api_key": "your_youtube_api_key",
  "channel_identifier": "UCBa659QWEk1AI4Tg--mrJ2A",
  "max_results": "100",
  "include_comments_count": false
}
```

### Incremental Sync Setup

```json
{
  "api_key": "your_youtube_api_key",
  "channel_identifier": "@NewsChannel",
  "start_date": "2024-01-01",
  "max_results": "all",
  "include_comments_count": true
}
```

## Data Schema

### Channel Stream Schema

```json
{
  "channel_id": "string",
  "title": "string",
  "description": "string",
  "custom_url": "string",
  "published_at": "string",
  "country": "string",
  "default_language": "string",
  "thumbnails": "object",
  "subscriber_count": "integer",
  "video_count": "integer",
  "view_count": "integer",
  "privacy_status": "string",
  "fetched_at": "string"
}
```

### Videos Stream Schema

```json
{
  "video_id": "string",
  "title": "string",
  "description": "string",
  "published_at": "string",
  "channel_id": "string",
  "channel_title": "string",
  "view_count": "integer",
  "like_count": "integer",
  "comment_count": "integer",
  "duration_seconds": "integer",
  "duration_iso": "string",
  "tags": "array",
  "category_id": "string",
  "privacy_status": "string",
  "fetched_at": "string"
}
```

## Troubleshooting

### Common Issues

**API Key Errors**
- Verify your API key is correct and active
- Ensure YouTube Data API v3 is enabled in Google Cloud Console
- Check that your API key has no IP restrictions that block Airbyte

**Channel Not Found**
- Verify the channel identifier format and spelling
- Ensure the channel is public and accessible
- Try different identifier formats (handle vs. channel ID)

**Quota Exceeded**
- Monitor your API usage in Google Cloud Console
- Consider increasing your quota limit if needed
- Use `max_results` to limit data fetching during testing

**Sync Failures**
- Check Airbyte logs for specific error messages
- Verify your internet connection and API accessibility
- Ensure the channel still exists and is accessible

### Debug Mode

Enable detailed logging in your Airbyte instance to troubleshoot connection issues:

1. Check connector logs in the Airbyte UI
2. Look for specific error messages and API responses
3. Verify configuration parameters

## Support

- **Documentation**: [YouTube Data API v3 Documentation](https://developers.google.com/youtube/v3)
- **API Console**: [Google Cloud Console](https://console.cloud.google.com/)
- **Airbyte Support**: Contact through your Airbyte instance or community channels

## Changelog

### Version 1.0.0
- Initial release with channel and videos streams
- Support for full refresh and incremental sync modes
- Comprehensive video and channel metadata extraction
- Rate limiting and error handling

## License

This connector is licensed under the MIT License.