from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timedelta
import logging
import requests
import re

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream, IncrementalMixin
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

logger = logging.getLogger("airbyte")

class PostsStream(Stream, IncrementalMixin):
    
    def __init__(self, identifier: str, password: str, search_terms: List[str] = None, limit: int = 25, **kwargs):
        super().__init__(**kwargs)
        self.identifier = identifier
        self.password = password
        self.service_url = "https://bsky.social"
        self.search_terms = search_terms or []
        self.limit = limit
        self._cursor_field = "indexed_at"
        self._state = {}
        self.session = None
    
    def _find_matching_term(self, post_text: str) -> Optional[str]:
        """Find which search term matched this post"""
        if not post_text or not self.search_terms:
            return None
        
        post_text_lower = post_text.lower()
        
        for term in self.search_terms:
            term_lower = term.lower()
            
            if term.startswith('#'):
                if term_lower in post_text_lower:
                    return term
            elif term.startswith('from:'):
                continue  
            else:
                if re.search(r'\b' + re.escape(term_lower) + r'\b', post_text_lower):
                    return term
        
        return self.search_terms[0] if self.search_terms else None
    
    @property
    def name(self) -> str:
        return "posts"
    
    @property
    def primary_key(self) -> Optional[str]:
        return "uri"
    
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
        return schema_loader.get_schema("posts")
    
    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        current_cursor = current_stream_state.get(self.cursor_field)
        latest_cursor = latest_record.get(self.cursor_field)
        
        if latest_cursor:
            if not current_cursor or latest_cursor > current_cursor:
                return {self.cursor_field: latest_cursor}
        
        return current_stream_state
    
    def _login(self):
        login_url = f"{self.service_url}/xrpc/com.atproto.server.createSession"
        
        response = requests.post(login_url, json={
            "identifier": self.identifier,
            "password": self.password
        })
        
        response.raise_for_status()
        session_data = response.json()
        
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {session_data['accessJwt']}"
        })
        
        return session_data
    
    def _search_for_term(self, term: str, limit_per_term: int) -> List[dict]:
        search_url = f"{self.service_url}/xrpc/app.bsky.feed.searchPosts"
        params = {
            "q": term,
            "limit": limit_per_term
        }
        
        response = self.session.get(search_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        posts = data.get('posts', [])
        
        for post in posts:
            post['_matched_term'] = term
        
        return posts
    
    def read_records(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        
        if not self.session:
            self._login()
        
        if not self.search_terms:
            logger.warning("No search terms provided")
            return
        
        try:
            logger.info(f"Searching Bluesky posts for terms: {self.search_terms}, limit per term: {self.limit}")
            
            all_posts = []
            seen_uris = set()  # To avoid duplicates
            
            limit_per_term = self.limit  
            
            for term in self.search_terms:
                logger.info(f"Searching for term: '{term}' (limit: {limit_per_term})")
                
                try:
                    posts = self._search_for_term(term, limit_per_term)
                    
                    for post in posts:
                        uri = post.get('uri')
                        if uri and uri not in seen_uris:
                            seen_uris.add(uri)
                            all_posts.append(post)
                        
                except Exception as e:
                    logger.warning(f"Error searching for term '{term}': {e}")
                    continue
            
            logger.info(f"Found {len(all_posts)} unique posts across all terms")
            
            for post in all_posts:
                post_text = post.get('record', {}).get('text', '')
                matched_term = post.get('_matched_term', self._find_matching_term(post_text))
                
                post_data = {
                    "uri": post.get('uri'),
                    "cid": post.get('cid'),
                    "author": {
                        "did": post.get('author', {}).get('did'),
                        "handle": post.get('author', {}).get('handle'),
                        "display_name": post.get('author', {}).get('displayName'),
                        "avatar": post.get('author', {}).get('avatar')
                    },
                    "record": {
                        "text": post_text,
                        "created_at": post.get('record', {}).get('createdAt'),
                        "langs": post.get('record', {}).get('langs')
                    },
                    "embed": post.get('embed'),
                    "reply_count": post.get('replyCount', 0),
                    "repost_count": post.get('repostCount', 0),
                    "like_count": post.get('likeCount', 0),
                    "indexed_at": post.get('indexedAt'),
                    "labels": post.get('labels'),
                    "search_term": matched_term
                }
                
                yield post_data
                
        except Exception as e:
            logger.error(f"Error fetching Bluesky posts: {e}")
            raise

class SourceBlueskyFetcher(AbstractSource):
    
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            identifier = config.get("identifier")
            password = config.get("password")
            
            if not identifier or not password:
                return False, "Identifier and password are required"
            
            login_url = "https://bsky.social/xrpc/com.atproto.server.createSession"
            response = requests.post(login_url, json={
                "identifier": identifier,
                "password": password
            })
            
            response.raise_for_status()
            return True, None
            
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        search_terms = config.get("search_terms", [])
        
        if not search_terms and config.get("search_query"):
            search_terms = [config.get("search_query")]
        
        return [
            PostsStream(
                identifier=config.get("identifier"),
                password=config.get("password"),
                search_terms=search_terms,
                limit=config.get("limit", 25)
            )
        ]
