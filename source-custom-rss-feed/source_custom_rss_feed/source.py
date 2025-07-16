from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timedelta
import logging
import feedparser
from urllib.parse import urlparse
from dateutil import parser as date_parser

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

logger = logging.getLogger("airbyte")

class ArticlesStream(Stream, IncrementalMixin):
    """
    Stream to extract articles from RSS feeds
    """
    
    def __init__(self, rss_urls: List[str], start_time: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.rss_urls = rss_urls if isinstance(rss_urls, list) else [rss_urls]
        self._cursor_field = "published"
        self._state = {}
        self.start_time = start_time
    
    @property
    def name(self) -> str:
        return "articles"
    
    @property
    def primary_key(self) -> Optional[str]:
        return "guid"
    
    @property
    def cursor_field(self) -> str:
        return self._cursor_field
    
    @property
    def state(self) -> Mapping[str, Any]:
        return self._state
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = value
    
    def get_json_schema(self) -> Mapping[str, Any]:
        schema_loader = ResourceSchemaLoader(package_name_from_class(self.__class__))
        return schema_loader.get_schema("articles")
    
    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_cursor = current_stream_state.get(self.cursor_field)
        latest_cursor = latest_record.get(self.cursor_field)
        
        if latest_cursor:
            if not current_cursor or latest_cursor > current_cursor:
                return {self.cursor_field: latest_cursor}
        
        return current_stream_state
    
    def _parse_date(self, date_string: str) -> Optional[str]:
        if not date_string:
            return None
            
        try:
            parsed_date = date_parser.parse(date_string)
            return parsed_date.isoformat()
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_string}': {e}")
            return date_string  # Return original if parsing fails
    
    def _extract_article_data(self, entry, feed_url: str) -> Mapping[str, Any]:
        published = None
        if hasattr(entry, 'published'):
            published = self._parse_date(entry.published)
        elif hasattr(entry, 'updated'):
            published = self._parse_date(entry.updated)
        
        content = None
        if hasattr(entry, 'content') and entry.content:
            content = entry.content[0].value if entry.content else None
        elif hasattr(entry, 'summary'):
            content = entry.summary
        
        description = None
        if hasattr(entry, 'summary'):
            description = entry.summary
        elif hasattr(entry, 'description'):
            description = entry.description
        
        author = None
        if hasattr(entry, 'author'):
            author = entry.author
        elif hasattr(entry, 'author_detail') and entry.author_detail:
            author = entry.author_detail.get('name')
        
        return {
            "author": author,
            "content": content,
            "description": description,
            "guid": getattr(entry, 'id', getattr(entry, 'guid', None)),
            "link": getattr(entry, 'link', None),
            "published": published,
            "title": getattr(entry, 'title', None),
            "_source_feed_url": feed_url
        }
    
    def read_records(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        current_cursor = stream_state.get(self.cursor_field) if stream_state else None
        
        for rss_url in self.rss_urls:
            logger.info(f"Fetching RSS feed: {rss_url}")
            
            try:
                feed = feedparser.parse(rss_url)
                
                if feed.bozo:
                    logger.warning(f"RSS feed parsing had issues for {rss_url}: {feed.bozo_exception}")
                
                for entry in feed.entries:
                    article_data = self._extract_article_data(entry, rss_url)
                    
                    if not article_data.get("guid"):
                        logger.warning(f"Skipping article without GUID from {rss_url}")
                        continue
                    
                    if current_cursor and article_data.get(self.cursor_field):
                        if article_data[self.cursor_field] <= current_cursor:
                            continue
                    
                    if self.start_time and article_data.get(self.cursor_field):
                        if article_data[self.cursor_field] < self.start_time:
                            continue
                    
                    yield article_data
                    
            except Exception as e:
                logger.error(f"Error processing RSS feed {rss_url}: {e}")
                continue


class SourceCustomRssFeed(AbstractSource):
    
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            rss_urls = config.get("rss_urls", [])
            if not rss_urls:
                return False, "No RSS URLs provided in configuration"
            
            test_url = rss_urls[0] if isinstance(rss_urls, list) else rss_urls
            feed = feedparser.parse(test_url)
            
            if feed.bozo and not feed.entries:
                return False, f"Failed to parse RSS feed: {feed.bozo_exception}"
            
            return True, None
            
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        rss_urls = config.get("rss_urls", [])
        start_time = config.get("start_time")
        
        # If no start_time provided, default to one week ago
        if not start_time:
            one_week_ago = datetime.utcnow() - timedelta(weeks=1)
            start_time = one_week_ago.strftime("%Y-%m-%dT%H:%M:%SZ")
            logger.info(f"No start_time provided, defaulting to one week ago: {start_time}")
        
        return [
            ArticlesStream(rss_urls=rss_urls, start_time=start_time)
        ]
