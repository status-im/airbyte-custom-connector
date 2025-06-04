import requests
import json
from typing import List, Dict, Optional, Iterator
from datetime import datetime, timezone
import time
import logging

logger = logging.getLogger(__name__)


class YouTubeDataAPI:
    """
    YouTube Data v3 API client for fetching channel videos with comprehensive statistics.
    """
    
    def __init__(self, api_key: str):
        """
        Initialize the YouTube Data API client.
        
        Args:
            api_key (str): YouTube Data API v3 key
        """
        self.api_key = api_key
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.session = requests.Session()
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """
        Make a request to the YouTube API with error handling and rate limiting.
        
        Args:
            endpoint (str): API endpoint
            params (Dict): Request parameters
            
        Returns:
            Dict: API response
        """
        params['key'] = self.api_key
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Handle rate limiting
            if response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting 60 seconds...")
                time.sleep(60)
                response = self.session.get(url, params=params)
                response.raise_for_status()
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_channel_id_by_username(self, username: str) -> Optional[str]:
        """
        Get channel ID from username.
        
        Args:
            username (str): YouTube channel username
            
        Returns:
            Optional[str]: Channel ID if found
        """
        params = {
            'part': 'id',
            'forUsername': username
        }
        
        response = self._make_request('channels', params)
        
        if response.get('items'):
            return response['items'][0]['id']
        return None
    
    def get_channel_id_by_handle(self, handle: str) -> Optional[str]:
        """
        Get channel ID from handle (e.g., @channelname).
        
        Args:
            handle (str): YouTube channel handle
            
        Returns:
            Optional[str]: Channel ID if found
        """
        # Remove @ if present
        if handle.startswith('@'):
            handle = handle[1:]
            
        params = {
            'part': 'id',
            'forHandle': handle
        }
        
        response = self._make_request('channels', params)
        
        if response.get('items'):
            return response['items'][0]['id']
        return None
    
    def get_channel_info(self, channel_id: str) -> Dict:
        """
        Get comprehensive channel information.
        
        Args:
            channel_id (str): YouTube channel ID
            
        Returns:
            Dict: Channel information
        """
        params = {
            'part': 'snippet,statistics,brandingSettings,contentDetails,topicDetails,status',
            'id': channel_id
        }
        
        response = self._make_request('channels', params)
        
        if response.get('items'):
            return response['items'][0]
        return {}
    
    def get_channel_videos_playlist_id(self, channel_id: str) -> Optional[str]:
        """
        Get the uploads playlist ID for a channel.
        
        Args:
            channel_id (str): YouTube channel ID
            
        Returns:
            Optional[str]: Uploads playlist ID
        """
        channel_info = self.get_channel_info(channel_id)
        
        if channel_info and 'contentDetails' in channel_info:
            return channel_info['contentDetails']['relatedPlaylists'].get('uploads')
        return None
    
    def get_playlist_videos(self, playlist_id: str, max_results: Optional[int] = None) -> Iterator[Dict]:
        """
        Get all videos from a playlist with pagination.
        
        Args:
            playlist_id (str): YouTube playlist ID
            max_results (Optional[int]): Maximum number of videos to fetch
            
        Yields:
            Dict: Video information
        """
        next_page_token = None
        videos_fetched = 0
        
        while True:
            params = {
                'part': 'snippet,contentDetails',
                'playlistId': playlist_id,
                'maxResults': min(50, max_results - videos_fetched) if max_results else 50
            }
            
            if next_page_token:
                params['pageToken'] = next_page_token
            
            response = self._make_request('playlistItems', params)
            
            for item in response.get('items', []):
                if max_results and videos_fetched >= max_results:
                    return
                    
                yield item
                videos_fetched += 1
            
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    
    def get_video_details(self, video_ids: List[str]) -> List[Dict]:
        """
        Get detailed information for multiple videos.
        
        Args:
            video_ids (List[str]): List of video IDs (max 50 per request)
            
        Returns:
            List[Dict]: Detailed video information
        """
        if not video_ids:
            return []
        
        # YouTube API allows max 50 video IDs per request
        video_ids = video_ids[:50]
        
        params = {
            'part': 'snippet,statistics,contentDetails,status,recordingDetails,topicDetails,localizations',
            'id': ','.join(video_ids)
        }
        
        response = self._make_request('videos', params)
        return response.get('items', [])
    
    def get_video_comments_count(self, video_id: str) -> int:
        """
        Get comment count for a specific video.
        
        Args:
            video_id (str): YouTube video ID
            
        Returns:
            int: Number of comments
        """
        try:
            params = {
                'part': 'snippet',
                'videoId': video_id,
                'maxResults': 1
            }
            
            response = self._make_request('commentThreads', params)
            return response.get('pageInfo', {}).get('totalResults', 0)
            
        except Exception as e:
            logger.warning(f"Could not fetch comments for video {video_id}: {e}")
            return 0
    
    def get_all_channel_videos(self, channel_identifier: str, max_results: Optional[int] = None, 
                             include_comments_count: bool = False) -> List[Dict]:
        """
        Get all videos from a channel with comprehensive data.
        
        Args:
            channel_identifier (str): Channel ID, username, or handle
            max_results (Optional[int]): Maximum number of videos to fetch
            include_comments_count (bool): Whether to fetch comment counts (slower)
            
        Returns:
            List[Dict]: Complete video data with statistics
        """
        # Determine if it's a channel ID, username, or handle
        channel_id = channel_identifier
        
        if channel_identifier.startswith('@'):
            channel_id = self.get_channel_id_by_handle(channel_identifier)
        elif not channel_identifier.startswith('UC'):
            # Assume it's a username
            channel_id = self.get_channel_id_by_username(channel_identifier)
        
        if not channel_id:
            raise ValueError(f"Could not find channel for identifier: {channel_identifier}")
        
        # Get uploads playlist ID
        uploads_playlist_id = self.get_channel_videos_playlist_id(channel_id)
        if not uploads_playlist_id:
            raise ValueError(f"Could not find uploads playlist for channel: {channel_id}")
        
        # Get channel info for context
        channel_info = self.get_channel_info(channel_id)
        
        all_videos = []
        video_batch = []
        
        logger.info(f"Fetching videos from channel: {channel_info.get('snippet', {}).get('title', channel_id)}")
        
        # Get videos from playlist
        for video_item in self.get_playlist_videos(uploads_playlist_id, max_results):
            video_id = video_item['snippet']['resourceId']['videoId']
            video_batch.append(video_id)
            
            # Process in batches of 50 (API limit)
            if len(video_batch) == 50:
                detailed_videos = self.get_video_details(video_batch)
                
                for video in detailed_videos:
                    enhanced_video = self._enhance_video_data(video, channel_info, include_comments_count)
                    all_videos.append(enhanced_video)
                
                video_batch = []
                
                # Rate limiting
                time.sleep(0.1)
        
        # Process remaining videos
        if video_batch:
            detailed_videos = self.get_video_details(video_batch)
            
            for video in detailed_videos:
                enhanced_video = self._enhance_video_data(video, channel_info, include_comments_count)
                all_videos.append(enhanced_video)
        
        logger.info(f"Successfully fetched {len(all_videos)} videos")
        return all_videos
    
    def _enhance_video_data(self, video: Dict, channel_info: Dict, include_comments_count: bool = False) -> Dict:
        """
        Enhance video data with additional computed fields and channel context.
        
        Args:
            video (Dict): Raw video data from API
            channel_info (Dict): Channel information
            include_comments_count (bool): Whether to fetch comment counts
            
        Returns:
            Dict: Enhanced video data
        """
        snippet = video.get('snippet', {})
        statistics = video.get('statistics', {})
        content_details = video.get('contentDetails', {})
        
        # Parse duration
        duration_iso = content_details.get('duration', 'PT0S')
        duration_seconds = self._parse_duration(duration_iso)
        
        # Parse published date
        published_at = snippet.get('publishedAt')
        published_datetime = None
        if published_at:
            published_datetime = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        
        enhanced_video = {
            'video_id': video['id'],
            'title': snippet.get('title', ''),
            'description': snippet.get('description', ''),
            'published_at': published_at,
            'published_datetime': published_datetime,
            'channel_id': snippet.get('channelId', ''),
            'channel_title': snippet.get('channelTitle', ''),
            'tags': snippet.get('tags', []),
            'category_id': snippet.get('categoryId', ''),
            'default_language': snippet.get('defaultLanguage', ''),
            'default_audio_language': snippet.get('defaultAudioLanguage', ''),
            'thumbnails': snippet.get('thumbnails', {}),
            
            # Statistics
            'view_count': int(statistics.get('viewCount', 0)),
            'like_count': int(statistics.get('likeCount', 0)),
            'comment_count': int(statistics.get('commentCount', 0)),
            'favorite_count': int(statistics.get('favoriteCount', 0)),
            
            # Content details
            'duration_iso': duration_iso,
            'duration_seconds': duration_seconds,
            'duration_formatted': self._format_duration(duration_seconds),
            'dimension': content_details.get('dimension', ''),
            'definition': content_details.get('definition', ''),
            'caption': content_details.get('caption', ''),
            'licensed_content': content_details.get('licensedContent', False),
            
            # Status
            'privacy_status': video.get('status', {}).get('privacyStatus', ''),
            'upload_status': video.get('status', {}).get('uploadStatus', ''),
            'license': video.get('status', {}).get('license', ''),
            'embeddable': video.get('status', {}).get('embeddable', False),
            'public_stats_viewable': video.get('status', {}).get('publicStatsViewable', False),
            
            # Channel context
            'channel_subscriber_count': int(channel_info.get('statistics', {}).get('subscriberCount', 0)),
            'channel_video_count': int(channel_info.get('statistics', {}).get('videoCount', 0)),
            'channel_view_count': int(channel_info.get('statistics', {}).get('viewCount', 0)),
            
            # Computed metrics
            'engagement_rate': self._calculate_engagement_rate(statistics),
            'views_per_subscriber': self._calculate_views_per_subscriber(
                statistics, channel_info.get('statistics', {})
            ),
            
            # Additional metadata
            'fetched_at': datetime.now(timezone.utc).isoformat(),
        }
        
        # Add topic details if available
        if 'topicDetails' in video:
            enhanced_video['topic_categories'] = video['topicDetails'].get('topicCategories', [])
            enhanced_video['relevant_topic_ids'] = video['topicDetails'].get('relevantTopicIds', [])
        
        # Add recording details if available
        if 'recordingDetails' in video:
            recording_details = video['recordingDetails']
            enhanced_video['recording_date'] = recording_details.get('recordingDate')
            enhanced_video['location_description'] = recording_details.get('locationDescription')
            if 'location' in recording_details:
                enhanced_video['recording_latitude'] = recording_details['location'].get('latitude')
                enhanced_video['recording_longitude'] = recording_details['location'].get('longitude')
                enhanced_video['recording_altitude'] = recording_details['location'].get('altitude')
        
        # Optionally fetch comment count separately for more accuracy
        if include_comments_count and enhanced_video['comment_count'] == 0:
            enhanced_video['comment_count'] = self.get_video_comments_count(video['id'])
        
        return enhanced_video
    
    def _parse_duration(self, duration_iso: str) -> int:
        """
        Parse ISO 8601 duration to seconds.
        
        Args:
            duration_iso (str): Duration in ISO 8601 format (e.g., PT4M13S)
            
        Returns:
            int: Duration in seconds
        """
        import re
        
        # Remove PT prefix
        duration = duration_iso[2:] if duration_iso.startswith('PT') else duration_iso
        
        # Extract hours, minutes, seconds
        hours = 0
        minutes = 0
        seconds = 0
        
        # Hours
        hours_match = re.search(r'(\d+)H', duration)
        if hours_match:
            hours = int(hours_match.group(1))
        
        # Minutes
        minutes_match = re.search(r'(\d+)M', duration)
        if minutes_match:
            minutes = int(minutes_match.group(1))
        
        # Seconds
        seconds_match = re.search(r'(\d+)S', duration)
        if seconds_match:
            seconds = int(seconds_match.group(1))
        
        return hours * 3600 + minutes * 60 + seconds
    
    def _format_duration(self, seconds: int) -> str:
        """
        Format duration seconds to HH:MM:SS format.
        
        Args:
            seconds (int): Duration in seconds
            
        Returns:
            str: Formatted duration
        """
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    
    def _calculate_engagement_rate(self, statistics: Dict) -> float:
        """
        Calculate engagement rate (likes + comments) / views.
        
        Args:
            statistics (Dict): Video statistics
            
        Returns:
            float: Engagement rate as percentage
        """
        views = int(statistics.get('viewCount', 0))
        likes = int(statistics.get('likeCount', 0))
        comments = int(statistics.get('commentCount', 0))
        
        if views == 0:
            return 0.0
        
        return ((likes + comments) / views) * 100
    
    def _calculate_views_per_subscriber(self, video_stats: Dict, channel_stats: Dict) -> float:
        """
        Calculate views per subscriber ratio.
        
        Args:
            video_stats (Dict): Video statistics
            channel_stats (Dict): Channel statistics
            
        Returns:
            float: Views per subscriber ratio
        """
        views = int(video_stats.get('viewCount', 0))
        subscribers = int(channel_stats.get('subscriberCount', 0))
        
        if subscribers == 0:
            return 0.0
        
        return views / subscribers
    
    def export_to_json(self, videos: List[Dict], filename: str) -> None:
        """
        Export video data to JSON file.
        
        Args:
            videos (List[Dict]): Video data to export
            filename (str): Output filename
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(videos, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Exported {len(videos)} videos to {filename}")
    
    def get_channel_analytics_summary(self, videos: List[Dict]) -> Dict:
        """
        Generate analytics summary for channel videos.
        
        Args:
            videos (List[Dict]): Video data
            
        Returns:
            Dict: Analytics summary
        """
        if not videos:
            return {}
        
        total_views = sum(video['view_count'] for video in videos)
        total_likes = sum(video['like_count'] for video in videos)
        total_comments = sum(video['comment_count'] for video in videos)
        total_duration = sum(video['duration_seconds'] for video in videos)
        
        avg_views = total_views / len(videos)
        avg_likes = total_likes / len(videos)
        avg_comments = total_comments / len(videos)
        avg_duration = total_duration / len(videos)
        
        # Find most popular video
        most_viewed = max(videos, key=lambda x: x['view_count'])
        most_liked = max(videos, key=lambda x: x['like_count'])
        most_commented = max(videos, key=lambda x: x['comment_count'])
        
        # Calculate engagement metrics
        avg_engagement = sum(video['engagement_rate'] for video in videos) / len(videos)
        
        return {
            'total_videos': len(videos),
            'total_views': total_views,
            'total_likes': total_likes,
            'total_comments': total_comments,
            'total_duration_seconds': total_duration,
            'total_duration_formatted': self._format_duration(total_duration),
            
            'average_views': avg_views,
            'average_likes': avg_likes,
            'average_comments': avg_comments,
            'average_duration_seconds': avg_duration,
            'average_duration_formatted': self._format_duration(int(avg_duration)),
            'average_engagement_rate': avg_engagement,
            
            'most_viewed_video': {
                'title': most_viewed['title'],
                'video_id': most_viewed['video_id'],
                'views': most_viewed['view_count']
            },
            'most_liked_video': {
                'title': most_liked['title'],
                'video_id': most_liked['video_id'],
                'likes': most_liked['like_count']
            },
            'most_commented_video': {
                'title': most_commented['title'],
                'video_id': most_commented['video_id'],
                'comments': most_commented['comment_count']
            },
            
            'channel_info': {
                'channel_id': videos[0]['channel_id'],
                'channel_title': videos[0]['channel_title'],
                'subscriber_count': videos[0]['channel_subscriber_count'],
                'total_channel_views': videos[0]['channel_view_count']
            }
        }


# Example usage and utility functions
def main():
    """
    Example usage of the YouTube Data API client.
    """
    import os
    
    # Get API key from environment variable
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        print("Please set YOUTUBE_API_KEY environment variable")
        return
    
    # Initialize the client
    youtube_client = YouTubeDataAPI(api_key)
    
    # Example: Get all videos from a channel
    channel_identifier = "@channelname"  # Can be channel ID, username, or handle
    
    try:
        print(f"Fetching videos from channel: {channel_identifier}")
        
        # Fetch all videos (you can limit with max_results parameter)
        videos = youtube_client.get_all_channel_videos(
            channel_identifier=channel_identifier,
            max_results=100,  # Remove or set to None for all videos
            include_comments_count=True  # Set to False for faster fetching
        )
        
        # Export to JSON
        youtube_client.export_to_json(videos, f"channel_videos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        # Generate analytics summary
        summary = youtube_client.get_channel_analytics_summary(videos)
        print("\nChannel Analytics Summary:")
        print(json.dumps(summary, indent=2, default=str))
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
