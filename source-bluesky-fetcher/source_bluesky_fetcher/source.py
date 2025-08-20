from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta
import logging
import requests
import re

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")

class BlueskyStream(HttpStream):
    url_base = "https://bsky.social"
    
    def __init__(self, search_terms: List[str] = None, limit: int = 25, **kwargs):
        super().__init__(**kwargs)
        self.search_terms = search_terms or []
        self.limit = limit
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"Accept": "application/json"}

class PostsStream(BlueskyStream):
    
    primary_key = "uri"
    cursor_field = "indexed_at"
    
    @property
    def name(self) -> str:
        return "posts" #otherwise it will lokk for the name of the classe (posts_stream) in the schema folder
    
    def path(self, **kwargs) -> str:
        return "/xrpc/app.bsky.feed.searchPosts"
    
    def _find_matching_term(self, post_text: str) -> Optional[str]:
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
    
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        # Bluesky API has a maximum limit of 100
        api_limit = min(self.limit, 100)
        params = {"limit": api_limit}
        if stream_slice and "search_term" in stream_slice:
            params["q"] = stream_slice["search_term"]
        return params
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        if not self.search_terms:
            yield {}
            return
            
        for term in self.search_terms:
            yield {"search_term": term}
    
    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        try:
            data = response.json()
            posts = data.get('posts', [])
            
            current_term = stream_slice.get("search_term") if stream_slice else None
            logger.info(f"Found {len(posts)} posts for term: {current_term}")
            
            for post in posts:
                post_text = post.get('record', {}).get('text', '')
                
                # Determine which term matched this post
                matched_term = current_term or self._find_matching_term(post_text)
                
                # Extract data (flattened for easier SQL queries)
                post_data = {
                    "uri": post.get('uri'),
                    "cid": post.get('cid'),
                    "author_did": post.get('author', {}).get('did'),
                    "author_handle": post.get('author', {}).get('handle'),
                    "author_display_name": post.get('author', {}).get('displayName'),
                    "author_avatar": post.get('author', {}).get('avatar'),
                    "text": post_text,
                    "created_at": post.get('record', {}).get('createdAt'),
                    "langs": post.get('record', {}).get('langs'),
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
            logger.error(f"Error parsing response: {e}")
            raise

class SourceBlueskyFetcher(AbstractSource):
    
    def _get_access_token(self, config: Mapping[str, Any]) -> str:
        login_url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        
        response = requests.post(login_url, json={
            "identifier": config.get("identifier"),
            "password": config.get("password")
        })
        
        response.raise_for_status()
        session_data = response.json()
        
        return session_data.get("accessJwt")
    
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            identifier = config.get("identifier")
            password = config.get("password")
            
            if not identifier or not password:
                return False, "Identifier and password are required"
            
            access_token = self._get_access_token(config)
            
            if not access_token:
                return False, "Failed to get access token"
            
            return True, None
            
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        search_terms = config.get("search_terms", [])
        
        if not search_terms and config.get("search_query"):
            search_terms = [config.get("search_query")]
        
        try:
            access_token = self._get_access_token(config)
            auth = TokenAuthenticator(token=access_token)
        except Exception as e:
            raise Exception(f"Authentication failed: {str(e)}. Check your credentials.")
        
        return [
            PostsStream(
                search_terms=search_terms,
                limit=config.get("limit", 25),
                authenticator=auth
            )
        ]
