"""
Source implementation for YouTube Data v3 API.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Generator, Any, List, Mapping, MutableMapping, Optional, Iterable, Tuple, Iterator

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.models import SyncMode, AirbyteCatalog, AirbyteStream

import json
import os
from .utils import YouTubeDataAPI

logger = logging.getLogger("airbyte")


class SourceYoutubeFetcher(AbstractSource):
  
    
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            api_key = config.get("api_key")
            if not api_key:
                return False, "Missing required field: api_key"
            
            # Initialize YouTube client
            youtube_client = YouTubeDataAPI(api_key)
          
            channel_identifier = config.get("channel_identifier")
            if not channel_identifier:
                return False, "Missing required field: channel_identifier"
     
            channel_id = channel_identifier
            if channel_identifier.startswith('@'):
                channel_id = youtube_client.get_channel_id_by_handle(channel_identifier)
            elif not channel_identifier.startswith('UC'):
                channel_id = youtube_client.get_channel_id_by_username(channel_identifier)
            
            if not channel_id:
                return False, f"Could not find channel for identifier: {channel_identifier}"
            
            # Test API connection
            channel_info = youtube_client.get_channel_info(channel_id)
            if not channel_info:
                return False, f"Could not retrieve channel information for: {channel_identifier}"
            
            logger.info(f"Successfully connected to YouTube channel: {channel_info.get('snippet', {}).get('title', channel_identifier)}")
            return True, None
            
        except Exception as e:
            return False, f"Error connecting to YouTube Data API: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            ChannelStream(config=config),
            VideoStream(config=config)
        ]


class ChannelStream(Stream):
    """Stream for YouTube channel statistics and information"""
    
    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        self.api_key = config.get("api_key")
        self.channel_identifier = config.get("channel_identifier")
        self.max_results = config.get("max_results")
        self.include_comments_count = config.get("include_comments_count", False)
        
        self.youtube_client = YouTubeDataAPI(self.api_key)

    @property
    def name(self) -> str:
        return "channel"

    @property
    def primary_key(self) -> Optional[List[str]]:
        return ["channel_id"]

    def get_json_schema(self) -> Mapping[str, Any]:
        schema_path = os.path.join(os.path.dirname(__file__), "schemas", "channel.json")
        logger.info(f"Looking for schema file at: {schema_path}")
        
        if not os.path.exists(schema_path):
            logger.error(f"Schema file does not exist: {schema_path}")
            # Return a default schema as fallback
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "fetched_at": {"type": "string"},
                    "channel_id": {"type": "string"},
                    "_metadata": {"type": "object"}
                }
            }
            
        try:
            with open(schema_path, "r") as file:
                schema = json.load(file)
                logger.info(f"Successfully loaded schema from {schema_path}")
                return schema
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in schema file {schema_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading schema from {schema_path}: {str(e)}")
            raise

    def get_channel_id(self) -> str:
        """Get the actual channel ID from the identifier"""
        channel_id = self.channel_identifier
        
        if self.channel_identifier.startswith('@'):
            channel_id = self.youtube_client.get_channel_id_by_handle(self.channel_identifier)
        elif not self.channel_identifier.startswith('UC'):
            channel_id = self.youtube_client.get_channel_id_by_username(self.channel_identifier)
        
        if not channel_id:
            raise ValueError(f"Could not find channel for identifier: {self.channel_identifier}")
        
        return channel_id

    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
       
        try:
            channel_id = self.get_channel_id()
            logger.info(f"Fetching channel information for: {channel_id}")
            
            # Get channel information
            channel_info = self.youtube_client.get_channel_info(channel_id)
            
            if not channel_info:
                logger.warning(f"No channel information found for: {channel_id}")
                return
            
            snippet = channel_info.get('snippet', {})
            statistics = channel_info.get('statistics', {})
            branding_settings = channel_info.get('brandingSettings', {})
            content_details = channel_info.get('contentDetails', {})
            topic_details = channel_info.get('topicDetails', {})
            status = channel_info.get('status', {})
            
            channel_record = {
                'channel_id': channel_info['id'],
                'title': snippet.get('title', ''),
                'description': snippet.get('description', ''),
                'custom_url': snippet.get('customUrl', ''),
                'published_at': snippet.get('publishedAt', ''),
                'country': snippet.get('country', ''),
                'default_language': snippet.get('defaultLanguage', ''),
                'thumbnails': snippet.get('thumbnails', {}),
                
                # Statistics
                'subscriber_count': int(statistics.get('subscriberCount', 0)),
                'video_count': int(statistics.get('videoCount', 0)),
                'view_count': int(statistics.get('viewCount', 0)),
                'hidden_subscriber_count': statistics.get('hiddenSubscriberCount', False),
                
                # Content details
                'uploads_playlist_id': content_details.get('relatedPlaylists', {}).get('uploads', ''),
                'likes_playlist_id': content_details.get('relatedPlaylists', {}).get('likes', ''),
                'favorites_playlist_id': content_details.get('relatedPlaylists', {}).get('favorites', ''),
                'watchHistory_playlist_id': content_details.get('relatedPlaylists', {}).get('watchHistory', ''),
                'watchLater_playlist_id': content_details.get('relatedPlaylists', {}).get('watchLater', ''),
                
                # Branding
                'channel_keywords': branding_settings.get('channel', {}).get('keywords', ''),
                'channel_unsubscribed_trailer': branding_settings.get('channel', {}).get('unsubscribedTrailer', ''),
                'channel_featured_channels_urls': branding_settings.get('channel', {}).get('featuredChannelsUrls', []),
                'banner_image_url': branding_settings.get('image', {}).get('bannerImageUrl', ''),
                
                # Topic details
                'topic_categories': topic_details.get('topicCategories', []),
                'topic_ids': topic_details.get('topicIds', []),
                
                # Status
                'privacy_status': status.get('privacyStatus', ''),
                'is_linked': status.get('isLinked', False),
                'long_uploads_status': status.get('longUploadsStatus', ''),
                'made_for_kids': status.get('madeForKids', False),
                'self_declared_made_for_kids': status.get('selfDeclaredMadeForKids', False),
                
                # Metadata
                'fetched_at': datetime.now(timezone.utc).isoformat(),
                'channel_identifier': self.channel_identifier,
                'api_quota_used': 1  # Channel info costs 1 quota unit
            }
            
            logger.info(f"Successfully fetched channel data for: {channel_record['title']}")
            yield channel_record
            
        except Exception as e:
            logger.error(f"Error fetching channel data: {str(e)}")
            raise


class VideoStream(Stream):
    """Stream for YouTube video statistics and information"""
    
    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        self.api_key = config.get("api_key")
        self.channel_identifier = config.get("channel_identifier")
        self.max_results = config.get("max_results")
        self.include_comments_count = config.get("include_comments_count", False)
        
        self.youtube_client = YouTubeDataAPI(self.api_key)

    @property
    def name(self) -> str:
        return "videos"

    @property
    def primary_key(self) -> Optional[List[str]]:
        return ["video_id"]

    def get_json_schema(self) -> Mapping[str, Any]:
        """Load JSON schema for the stream"""
        schema_path = os.path.join(os.path.dirname(__file__), "schemas", "videos.json")
        logger.info(f"Looking for schema file at: {schema_path}")
        
        if not os.path.exists(schema_path):
            logger.error(f"Schema file does not exist: {schema_path}")
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": {
                    "fetched_at": {"type": "string"},
                    "video_id": {"type": "string"},
                    "_metadata": {"type": "object"}
                }
            }
            
        try:
            with open(schema_path, "r") as file:
                schema = json.load(file)
                logger.info(f"Successfully loaded schema from {schema_path}")
                return schema
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error in schema file {schema_path}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading schema from {schema_path}: {str(e)}")
            raise

    def get_channel_id(self) -> str:
        channel_id = self.channel_identifier
        
        if self.channel_identifier.startswith('@'):
            channel_id = self.youtube_client.get_channel_id_by_handle(self.channel_identifier)
        elif not self.channel_identifier.startswith('UC'):
            channel_id = self.youtube_client.get_channel_id_by_username(self.channel_identifier)
        
        if not channel_id:
            raise ValueError(f"Could not find channel for identifier: {self.channel_identifier}")
        
        return channel_id

    def read_records(self, sync_mode: SyncMode, stream_slice: Optional[Mapping[str, Any]] = None, 
                     stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        try:
            channel_id = self.get_channel_id()
            logger.info(f"Fetching videos for channel: {channel_id}")
            
            max_results = None
            if self.max_results and self.max_results != "all":
                try:
                    max_results = int(self.max_results)
                except ValueError:
                    logger.warning(f"Invalid max_results value: {self.max_results}, fetching all videos")
            
            videos = self.youtube_client.get_all_channel_videos(
                channel_identifier=self.channel_identifier,
                max_results=max_results,
                include_comments_count=self.include_comments_count
            )
            
            if not videos:
                logger.warning(f"No videos found for channel: {channel_id}")
                return
            
            logger.info(f"Successfully fetched {len(videos)} videos from channel")
            
            # Process each video
            for video in videos:
                # The video data is already comprehensive from the API client
                # Add some additional metadata for Airbyte
                video_record = {
                    **video,  # Include all existing video data
                    '_airbyte_channel_identifier': self.channel_identifier,
                    '_airbyte_sync_time': datetime.now(timezone.utc).isoformat(),
                }
                
                yield video_record
                
        except Exception as e:
            logger.error(f"Error fetching video data: {str(e)}")
            raise

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], 
                         latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        # Use published_at as the cursor for incremental sync
        published_at = latest_record.get('published_at')
        if published_at:
            current_stream_state['published_at'] = published_at
        return current_stream_state

    def stream_slices(self, sync_mode: SyncMode, cursor_field: List[str] = None, 
                     stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
      
        # For YouTube videos, we use a single slice since we fetch all videos at once
        yield None

    def supports_incremental(self) -> bool:
        return True

    @property
    def cursor_field(self) -> str:
        return "published_at"

    @property
    def source_defined_cursor(self) -> bool:
        return True
