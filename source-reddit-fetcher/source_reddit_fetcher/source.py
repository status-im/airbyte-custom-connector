from abc import ABC
from typing import Any, Iterable, List, Mapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
import logging, json, requests
from datetime import datetime, timezone
import requests.auth
import pandas as pd
import time
import re

logger = logging.getLogger("airbyte")
BASE_URL = "https://www.reddit.com"

class RedditCredentialsAuthentication(TokenAuthenticator):

    def __init__(self, client_id: str, client_secret: str, username: str, **kwargs):
        headers = {
            "User-Agent": f"python:app.client_credentials:v1.0 (by u/{username})"
        }
        data    = {
            "grant_type": "client_credentials"
        }

        # https://github.com/reddit-archive/reddit/wiki/oauth2#authorization
        url = f"{BASE_URL}/api/v1/access_token"
        logger.info(f"Authentication URL: {url}")

        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        response = requests.post(url, auth=auth, data=data, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully connected to {url}")
        info: dict = response.json()

        token: str = info.get("access_token")
        auth_method: str = info.get("token_type")
        if not token:
            raise Exception("Could not fetch access token... Please further investigate!")
        logger.info("Successfully fetched Reddit access token")
        logger.info(f"Authentication method: {auth_method.title()}")
        valid_hours = info["expires_in"] / (60 * 60)
        logger.info(f"Token is valid for: {int(valid_hours)} hours")

        super().__init__(token, auth_method.title())


class RedditStream(HttpStream, ABC):

    primary_key: Optional[str] = None
    url_base = "https://oauth.reddit.com/"

    # Reddit API rate limits: 60 requests per minute for OAuth
    _min_request_interval = 1.1  # Slightly more than 1 second to be safe

    def __init__(self, days: int, subreddit: str, authenticator: requests.auth.AuthBase):
        super().__init__(authenticator=authenticator)

        self.subreddit = subreddit
        today_utc = datetime.now(timezone.utc).date()
        self.start_date = (today_utc - pd.offsets.Day(days)).date()
        self._last_request_time = None  # Instance variable for proper rate limiting

    @property
    def http_method(self) -> str:
        return "GET"

    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {}

    def request_headers(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        headers = super().request_headers(stream_state, stream_slice, next_page_token)
        # Ensure proper User-Agent for all requests
        headers["User-Agent"] = f"python:airbyte-reddit-connector:v1.0 (by u/airbyte-user)"
        return headers

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        # Handle 429 rate limit responses
        if response.status_code == 429:
            # First try to parse from headers
            wait = response.headers.get("Retry-After")
            if wait:
                wait_time = float(wait)
            else:
                # Try to parse from response text
                if response.text:
                    # Look for "please wait X second(s)" pattern
                    match = re.search(r'please wait (\d+) second', response.text, re.IGNORECASE)
                    if match:
                        wait_time = float(match.group(1))
                    else:
                        wait_time = 10.0
                else:
                    wait_time = 10.0

            logger.warning(f"Rate limited (429)! Waiting {wait_time}s before retry...")
            return wait_time

        # For other error responses, use exponential backoff
        if response.status_code >= 500:
            return 5.0

        return None

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        # Implement request rate limiting
        if self._last_request_time:
            time_since_last = time.time() - self._last_request_time
            if time_since_last < self._min_request_interval:
                sleep_time = self._min_request_interval - time_since_last
                logger.info(f"Rate limiting: sleeping {sleep_time:.2f}s between requests")
                time.sleep(sleep_time)

        self._last_request_time = time.time()
        return super()._send_request(request, request_kwargs)

    def to_utc_timestamp(self, timestamp: float) -> datetime:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def request_params(self, stream_state: Optional[Mapping[str, Any]], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        # Use smaller limit to reduce rate limiting
        params = {
            "limit": 25
        }
        if next_page_token:
            params.update(next_page_token)

        return params

class MultiSubredditPosts(RedditStream):
    """Unified posts stream that handles multiple subreddits"""

    type_prefixes = {
        "t1": "comment",
        "t2": "account",
        "t3": "link",
        "t4": "message",
        "t5": "subreddit",
        "t6": "award"
    }

    primary_key = "id"
    cursor_field = "created_timestamp"

    def __init__(self, days: int, subreddits: List[str], authenticator: requests.auth.AuthBase):
        # Use first subreddit for parent initialization
        super().__init__(days=days, subreddit=subreddits[0], authenticator=authenticator)
        self.subreddits = subreddits
        self._last_posts = {}
        self._last_ids = {}

    @property

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        """Create slices for each subreddit"""
        for subreddit in self.subreddits:
            yield {"subreddit": subreddit}

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        subreddit = stream_slice["subreddit"] if stream_slice else self.subreddit
        return f"r/{subreddit}/new"

    def parse_response(self, response: requests.Response, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        subreddit = stream_slice["subreddit"] if stream_slice else self.subreddit
        data: dict[str, Any] = response.json()
        children: list[dict] = data.get("data", {}).get("children", [])

        for child in children:
            data = child.get("data", {})
            if not data:
                continue

            try:
                created_utc = data.get("created_utc")
                if created_utc:
                    created_timestamp = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                    self._last_posts[subreddit] = created_timestamp.date()
                else:
                    created_timestamp = None
                    logger.debug(f"Post {data.get('id', 'unknown')} has no created_utc timestamp")

                # Use post ID or generate a unique identifier
                post_id = data.get("id", f"unknown_{hash(str(data))}")

                row = {
                    "id": f"{subreddit}-{post_id}",
                    "kind_tag": child.get("kind", ""),
                    "kind_name": self.type_prefixes.get(child.get("kind", ""), "unknown"),
                    "subreddit": subreddit,
                    "post_id": post_id,
                    "post_url": BASE_URL + data.get("permalink", ""),
                    "url": data.get("url", ""),
                    "domain": data.get("domain", ""),
                    "created_timestamp": created_timestamp,
                    "timezone": "UTC",
                    "title": data.get("title", ""),
                    "text": data.get("selftext", ""),
                    "html_text": data.get("selftext_html", ""),
                    "author": data.get("author", ""),
                    "author_fullname": data.get("author", ""),
                    "downs": data.get("downs", 0),
                    "ups": data.get("ups", 0),
                    "score": data.get("score", 0),
                    "upvote_ratio": data.get("upvote_ratio", 0.0),
                    "subreddit_subscribers": data.get("subreddit_subscribers", 0),
                    "raw": json.dumps(data)
                }
                yield row

                if created_utc:
                    self._last_ids[subreddit] = f"t5_{post_id}"

            except Exception as e:
                logger.warning(f"Failed to parse post {data.get('id', 'unknown')} from r/{subreddit}: {str(e)}")
                continue

    def next_page_token(self, response: requests.Response, stream_slice: Mapping[str, Any] = None) -> Optional[dict[str, Any]]:
        subreddit = stream_slice["subreddit"] if stream_slice else self.subreddit

        if subreddit in self._last_posts and self.start_date < self._last_posts[subreddit]:
            return {"before": self._last_ids[subreddit]}


class MultiSubredditComments(HttpSubStream):
    """Unified comments stream that handles multiple subreddits"""

    primary_key = "id"
    cursor_field = "created_timestamp"
    url_base = "https://oauth.reddit.com/"
    _min_request_interval = 1.1

    def __init__(self, days: int, subreddits: List[str], authenticator: requests.auth.AuthBase, parent):
        super().__init__(parent=parent, authenticator=authenticator)
        self.subreddits = subreddits
        self.days = days
        today_utc = datetime.now(timezone.utc).date()
        self.start_date = (today_utc - pd.offsets.Day(days)).date()
        self._last_request_time = None

    @property
    def http_method(self) -> str:
        return "GET"

    def get_updated_state(self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {}

    def request_headers(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        headers = super().request_headers(stream_state, stream_slice, next_page_token)
        headers["User-Agent"] = f"python:airbyte-reddit-connector:v1.0 (by u/airbyte-user)"
        return headers

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code == 429:
            wait = response.headers.get("Retry-After")
            if wait:
                wait_time = float(wait)
            else:
                if response.text:
                    match = re.search(r'please wait (\d+) second', response.text, re.IGNORECASE)
                    if match:
                        wait_time = float(match.group(1))
                    else:
                        wait_time = 10.0
                else:
                    wait_time = 10.0

            logger.warning(f"Rate limited (429)! Waiting {wait_time}s before retry...")
            return wait_time

        if response.status_code >= 500:
            return 5.0

        return None

    def _send_request(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        if self._last_request_time:
            time_since_last = time.time() - self._last_request_time
            if time_since_last < self._min_request_interval:
                sleep_time = self._min_request_interval - time_since_last
                logger.info(f"Rate limiting: sleeping {sleep_time:.2f}s between requests")
                time.sleep(sleep_time)

        self._last_request_time = time.time()
        return super()._send_request(request, request_kwargs)

    def to_utc_timestamp(self, timestamp: float) -> datetime:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def request_params(self, stream_state: Optional[Mapping[str, Any]], stream_slice: Optional[Mapping[str, Any]] = None, next_page_token: Optional[Mapping[str, Any]] = None):
        params = {
            "limit": 25
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    @property

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        post: dict = stream_slice.get("parent")
        post_id = post["post_id"]
        subreddit = post["subreddit"]
        return f"r/{subreddit}/comments/{post_id}"

    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        _, comments = response.json()
        post_id = response.url.split("/")[-1].split("?")[0]
        subreddit = stream_slice.get("parent", {}).get("subreddit", "unknown")

        for child in comments["data"]["children"]:
            child_data: dict = child["data"]

            try:
                created_utc = child_data.get("created_utc")
                if created_utc:
                    created_timestamp = self.to_utc_timestamp(created_utc)
                else:
                    created_timestamp = None
                    logger.debug(f"Comment has no created_utc timestamp")

                comment_id = child_data.get("id", f"unknown_{hash(str(child_data))}")

                row = {
                    "id": f"{subreddit}-{post_id}-{comment_id}",
                    "post_id": f"{subreddit}-{post_id}",
                    "subreddit": subreddit,
                    "comment_id": comment_id,
                    "created_timestamp": created_timestamp,
                    "timezone": "UTC",
                    "parent_id": child_data.get("parent_id", "").split("_")[-1] if child_data.get("parent_id") else "",
                    "author": child_data.get("author", ""),
                    "text": child_data.get("body", ""),
                    "html_text": child_data.get("body_html", ""),
                    "url": BASE_URL + child_data.get("permalink", ""),
                    "ups": child_data.get("ups", 0),
                    "downs": child_data.get("downs", 0),
                    "score": child_data.get("score", 0)
                }
                yield row
            except Exception as e:
                logger.warning(f"Failed to parse comment {child_data.get('id', 'unknown')} from r/{subreddit}: {str(e)}")
                continue

    def next_page_token(self, response: requests.Response):
        _, comments = response.json()
        before = comments.get("data", {}).get("before")
        return {"before": before} if before else None


class SourceRedditFetcher(AbstractSource):

    def __init__(self):
        super().__init__()

    def check_connection(self, logger: logging.Logger, config: dict) -> Tuple[bool, Any]:
        logger.info(f"Config keys: {config.keys()}")
        auth = RedditCredentialsAuthentication(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            username=config["username"]
        )

        subreddits = config.get("subreddits", [])
        if not subreddits and config.get("subreddit"):
            # Backward compatibility: convert single subreddit to list
            subreddits = [config["subreddit"]]

        if not subreddits:
            return False, "No subreddits specified in configuration"

        # Test connection with the first subreddit
        test_subreddit = subreddits[0]
        url = BASE_URL + f"/r/{test_subreddit}/new"
        logger.info(f"Testing connection with: {url}")
        resp = requests.get(url, auth=auth)
        resp.raise_for_status()
        logger.info(f"Successfully connected to Reddit API. Will monitor {len(subreddits)} subreddit(s): {', '.join(subreddits)}")
        return resp.status_code == 200, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = RedditCredentialsAuthentication(
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            username=config["username"]
        )

        subreddits = config.get("subreddits", [])
        if not subreddits and config.get("subreddit"):
            subreddits = [config["subreddit"]]

        if not subreddits:
            # If no subreddits configured, raise an error
            raise ValueError("No subreddits specified in configuration. Please provide either 'subreddits' array or 'subreddit' string.")

        logger.info(f"Creating unified streams for {len(subreddits)} subreddit(s): {', '.join(subreddits)}")

        posts_stream = MultiSubredditPosts(days=config["days"], subreddits=subreddits, authenticator=auth)
        comments_stream = MultiSubredditComments(days=config["days"], subreddits=subreddits, authenticator=auth, parent=posts_stream)

        streams = [posts_stream, comments_stream]
        logger.info(f"Created unified streams: {[stream.name for stream in streams]}")
        return streams
