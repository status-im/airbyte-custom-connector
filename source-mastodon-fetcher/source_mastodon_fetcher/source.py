from typing import Any, Iterable, List, Mapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
import logging, datetime, requests, time
import requests.auth
import pandas as pd

logger = logging.getLogger("airbyte")

def join(url_base: str, *parts) -> str:
    """
    Join the URL parts and remove duplicated slashes

    Parameters:
        - `url_base` - the base URL of the API
        - `*parts` - API endpoint. Can be either one full path or chunks of it
    
    Output:
        - the concatanated URL
    """
    if url_base.endswith("/"):
        url_base = url_base[:-1]

    updated = []
    for part in parts:
        
        if part.endswith("/"):
            part = part[:-1]
        
        if part.startswith("/"):
            part = part[1:]
    
        updated.append(part)
    
    return "/".join([url_base, *updated])

class MastodonStream(HttpStream):
    primary_key = "id"
    cursor_field = "created_at"
    # https://docs.joinmastodon.org/methods/timelines/
    MAX_REQUESTS  = 40
    # Picked based on vibes
    REMAINING_THRESHOLD = 10

    def __init__(self, url_base: str, days: int, authenticator: requests.auth.AuthBase):
        super().__init__(authenticator=authenticator)
        self.days = days
        today_utc = datetime.datetime.now(datetime.timezone.utc).date()
        # Pandas allows us to deal with custom date ranges
        self.start_date = (today_utc - pd.offsets.Day(days)).date()
        self._url_base = url_base
        self.timezone = "UTC"
    
    @property
    def url_base(self) -> str:
        return self._url_base
    
    def join(self, *parts) -> str:
        """
        Custom function
        
        Create the full REST API url and remove additional forward slashes.

        Parameters:
            - `*parts` - the parts that should be included after the `url_base`

        Output:
            - the concatanated URL
        """
        return join(self.url_base, *parts)
    
    def next_page_token(self, response: requests.Response) -> Optional[dict[str, Any]]:
        data: list[dict] = response.json()
        if len(data) == 0 or self.is_old(response):
            return None
        
        remaining_ratelimit = int(response.headers["x-ratelimit-remaining"])
        if remaining_ratelimit < self.REMAINING_THRESHOLD:
            sleep_seconds = self.backoff_time(response)
            time.sleep(sleep_seconds)
        
        earliest_post = data[-1]
        
        return {"max_id": earliest_post["id"]}
    
    def request_params(self, stream_state=None, next_page_token=None, **kwargs):
        params = {"limit": self.MAX_REQUESTS}
        if next_page_token:
            params.update(next_page_token) 
        return params

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        # Time is in UTC
        ratelimit_reset = self.to_datetime(response.headers["x-ratelimit-reset"])
        seconds = (ratelimit_reset - datetime.datetime.now(datetime.timezone.utc)).seconds
        logger.info(f"Waiting {seconds}s until x-ratelimit-reset")
        return seconds
    
    def to_datetime(self, utc_date: str) -> datetime.datetime:
        """
        Custom function

        Convert the string dates to UTC datetimes.

        Parameters:
            - `utc_date` - Mastodon date. By default they are in UTC timezone.

        Output:
            - the converted date with UTC timezone
        """
        return datetime.datetime.fromisoformat(utc_date.replace("Z", "+00:00"))

    def is_old(self, response: requests.Response) -> bool:
        """
        Custom function

        Check if the given request is within the specified config day range.

        Parameters:
            - `response` - the REST API response

        Output:
            - when `True` the data should not be uploaded / the `request_params` should be None
        """
        if self.days <= 0:
            return False
        
        data: list[dict] = response.json()

        earliest_post = data[-1]
        earliest_date = self.to_datetime(earliest_post["created_at"]).date()

        return earliest_date <= self.start_date



class TagFeed(MastodonStream):

    def __init__(self, url_base: str, tags: list[str], days: int, authenticator: requests.auth.AuthBase):
        super().__init__(url_base, days, authenticator)
        self.tags = tags

    def stream_slices(self, **kwargs):
        for tag in self.tags:
            yield {"tag": tag}

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        tag = stream_slice["tag"]
        return self.join("/api/v1/timelines/tag/", tag)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if self.is_old(response):
            return
        
        tag = response.url.split("/")[-1].split("?")[0]
        posts: list[dict] = response.json()
        for post in posts:
            post.update({
                "timezone": self.timezone,
                "api_tag": tag,
                "created_at": datetime.datetime.fromisoformat(post["created_at"].replace("Z", "+00:00"))
            })            
            yield post



class AccountFeed(MastodonStream):

    def __init__(self, url_base: str, account_ids: list[str], days: int, authenticator: requests.auth.AuthBase):
        super().__init__(url_base, days, authenticator)
        self.account_ids = account_ids

    def stream_slices(self, **kwargs):
        for account_id in self.account_ids:
            yield {"account_id": account_id}

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        account_id = stream_slice["account_id"]
        return self.join("/api/v1/accounts/", account_id, "statuses")
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if self.is_old(response):
            return
        
        posts: list[dict] = response.json()
        for post in posts:
            post.update({
                "timezone": self.timezone,
                "created_at": datetime.datetime.fromisoformat(post["created_at"].replace("Z", "+00:00"))
            })            
            yield post



class SourceMastodonFetcher(AbstractSource):

    def __init__(self):
        super().__init__()

    def check_connection(self, logger: logging.Logger, config: dict) -> Tuple[bool, Any]:
        url = join(config["url_base"], "/api/v1/announcements")
        headers = {
            "Authorization": f"Bearer " + config["access_token"]
        }
        response = requests.get(url, headers=headers)
        logger.info(f"Status code: {response.status_code}")
        return response.status_code == 200, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        auth = TokenAuthenticator(config["access_token"])        
        account_ids = [self.__get_account_id(username, config) for username in config["accounts"]]
        streams = [
            TagFeed(config["url_base"], config["tags"], config["days"], auth),
            AccountFeed(config["url_base"], account_ids, config["days"], auth)
        ]
        return streams
    
    def __get_account_id(self, username: str, config: dict) -> Optional[str]:
        """
        Get the Mastadon account ID for the given username

        Parameters:
            - `username` - the Mastadon username
            - `config` - the Airbyte config

        Output:
            - the account ID if it exists
        """
        headers = {"Authorization": f"Bearer " + config["access_token"]}

        url = join(config["url_base"], "/api/v1/accounts/lookup")
        params = {"acct": username}
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()

        return resp.json().get("id", None)       