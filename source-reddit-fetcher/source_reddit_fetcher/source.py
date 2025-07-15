from typing import Any, Iterable, List, Mapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
import logging, json, datetime, requests
import requests.auth
import pandas as pd

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

        url = f"{BASE_URL}/api/v1/access_token"
        logger.info(f"Authentication URL: {url}")

        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        response = requests.post(url, auth=auth, data=data, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully connected to {url}")
        token = response.json().get("access_token")
        
        if not token:
            raise Exception("Could not fetch access token... Please further investigate!")
        
        logger.info("Successfully fetched Reddit access token")
        super().__init__(token) 



class ApiStream(HttpStream):

    primary_key: Optional[str] = None
    url_base = "https://oauth.reddit.com/"

    def __init__(self, authenticator: requests.auth.AuthBase):
        super().__init__(authenticator=authenticator)


    def backoff_time(self, response: requests.Response) -> Optional[float]:
        
        wait = response.headers.get("Retry-After")
        if not wait:
            wait = response.headers.get("x-ratelimit-reset")
        
        if response.status_code == 429:
            logger.warning(f"Raised too many requests at once! Waiting {wait}s...")
        
        return float(wait) if wait else None


    def to_utc_timestamp(self, timestamp: float) -> datetime.date:
        return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).date()


class SubredditPosts(ApiStream):

    # Based on "type prefixes"
    # https://www.reddit.com/dev/api/
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

    def __init__(self, subreddit: str, authenticator: requests.auth.AuthBase):
        super().__init__(authenticator)
        self.subreddit = subreddit



    def path(self, **kwargs):
        return f"{self.url_base}/r/{self.subreddit}/new"



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        data: dict[str, Any] = response.json()
        children: list[dict] = data.get("data", {}).get("children", [])

        for child in children:
            
            data = child.get("data", {})
            if not data:
                continue
            
            row = {
                "id": f"{self.subreddit}-{data['id']}",
                "kind_tag": child["kind"],
                "kind_name": self.type_prefixes[child["kind"]],
                "subreddit": self.subreddit,
                "post_id": data["id"],
                "post_url": BASE_URL + data["permalink"],
                "url": data["url"],
                "domain": data["domain"],
                "created_timestamp": datetime.datetime.fromtimestamp(data["created_utc"], tz=datetime.timezone.utc),
                "timezone": "UTC",
                "title": data["title"],
                "text": data["selftext"],
                "html_text": data["selftext_html"],
                "author": data["author"],
                "raw": json.dumps(data)
            }
            yield row



    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:
        data: dict = response.json()
        after = data.get("data", {}).get("after")
        return {"after": after} if after else None



class SubredditVotes(SubredditPosts):

    primary_key = "id"
    cursor_field = ""

    def __init__(self, days: int, subreddit: str, authenticator: requests.auth.AuthBase):
        super().__init__(subreddit, authenticator)
        
        today_utc = datetime.datetime.now(datetime.timezone.utc).date()
        self.start_date = (today_utc - pd.offsets.Day(days)).date()


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        
        data: dict[str, Any] = response.json()
        children: list[dict] = data.get("data", {}).get("children", [])

        for child in children:
            
            data = child.get("data", {})
            if not data:
                continue
                        
            created_utc = self.to_utc_timestamp(data["created_utc"])
            if self.start_date > created_utc:
                break

            keys = ["ups", "downs", "upvote_ratio", "score"]
            
            row = {
                "id": f"{str(datetime.datetime.now().timestamp()).replace('.', '')}-{self.subreddit}-{data['id']}",
                "post_id": f"{self.subreddit}-{data['id']}",
                "kind": child["kind"],
                "kind_name": self.type_prefixes[child["kind"]],
                **{key: data[key] for key in keys},
            }

            yield row



    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:
        
        params = super().next_page_token(response)

        if not params:
            return None
        
        data: dict[str, Any] = response.json()
        children: list[dict] = data.get("data", {}).get("children", [])

        if self.start_date > self.to_utc_timestamp(children[-1]["data"]["created_utc"]):
            params = None

        return params



class SubredditComments(HttpSubStream, ApiStream):

    def __init__(self, subreddit: str, authenticator: requests.auth.AuthBase, **kwargs):
        super().__init__(parent=SubredditPosts, **kwargs)
        
        self.subreddit = subreddit
        self.parent = SubredditPosts(self.subreddit, authenticator)

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        post: dict = stream_slice.get("parent")
        post_id = post["post_id"]

        url = f"{self.url_base}/r/{self.subreddit}/comments/{post_id}"
        logger.info(f"{self.__class__.__name__}: {url}")
        return url


    def parse_response(self, response: requests.Response, *, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None):
        
        _, comments = response.json()
        post_id = response.url.split("/")[-1].split("?")[0]

        for child in comments["data"]["children"]:
            child_data: dict = child["data"]

            row = {
                "id": f"{self.subreddit}-{post_id}-{child_data['id']}",
                "post_id": f"{self.subreddit}-{post_id}",
                "subreddit": self.subreddit,
                "comment_id": child_data['id'],
                "created_timestamp": self.to_utc_timestamp(child_data["created_utc"]),
                "timezone": "UTC",
                "parent_id": child_data["parent_id"].split("_")[1],
                "author": child_data.get("author"),
                "text": child_data["body"],
                "html_text": child_data["body_html"],
                "url": BASE_URL + child_data["permalink"]
            }
            yield row



class SourceRedditFetcher(AbstractSource):

    def __init__(self):
        super().__init__()

    def check_connection(self, logger: logging.Logger, config: dict) -> Tuple[bool, Any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        
        auth = RedditCredentialsAuthentication(**config)
        streams = [
            SubredditPosts(subreddit=config["subreddit"], authenticator=auth),
            SubredditVotes(days=config["days"], subreddit=config["subreddit"], authenticator=auth)
        ]
        return streams