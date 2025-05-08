from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
import time
from datetime import datetime, timedelta

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")

LOCATION_KEYS=["id", "rank", "name", "downloads_total", "downloads_percent"]
DOWNLOADS_KEY=["interval", "downloads_total", "downloads_percent"]
TECH_KEY=["rank", "name", "downloads_total", "downloads_percent"]

# Basic full refresh stream
class SimplecastFectherStream(HttpStream):
    url_base = "https://api.simplecast.com/"
    primary_key = None


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        pages = response.json().get('pages')
        if pages and pages.get('next'):
            time.sleep(2)
            return {
                'limit': pages.get('limit'),
                'offset': pages.get('limit')* pages.get('current')
            }

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        if next_page_token:
            return next_page_token


class Podcast(SimplecastFectherStream):

    primary_key = "id"


    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "podcasts"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        for elt in data.get('collection'):
            podcast={
                "id": elt.get("id"),
                "title": elt.get("title"),
                "status": elt.get("status"),
                "href": elt.get("href"),
                "episode_count": elt.get("episodes").get("count"),
                "account_id": elt.get("account_id"),
                "account_owner_name": elt.get("account").get("owner").get("name")
            }
            yield podcast

class Episode(HttpSubStream, SimplecastFectherStream):
    primary_key="id"

    @property
    def use_cache(self) -> bool:
        return True

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"podcasts/{podcast_id}/episodes"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        for elt in data.get('collection'):
            episode={
                "id": elt.get("id"),
                "title": elt.get("title"),
                "status": elt.get("status"),
                "published_at": elt.get("published_at"),
                "updated_at": elt.get("updated_at"),
                "scheduled_for": elt.get("scheduled_for"),
                "season": elt.get('season'),
                "number": elt.get("number"),
                "description": elt.get("description"),
                "token": elt.get("token"),
                "type": elt.get("type")
            }
            yield episode


class EpisodeDownload(HttpSubStream, SimplecastFectherStream):
    primary_key=None

    def __init__(self, sync_mode:bool=False, **kwargs):
        super().__init__(**kwargs)
        self.sync_mode = sync_mode

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        episode_id=stream_slice.get("parent").get("id")
        start_date=""
        if not self.sync_mode:
            start_date=f"&start_date={(datetime.now() - timedelta(days=1)).date().isoformat()}"
        return f"analytics/downloads?episode={episode_id}{start_date}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        episode=data.get('id')
        for elt in data.get("by_interval"):
            analytic={ key: elt.get(key) for key in DOWNLOADS_KEY }
            analytic['episode_id']=episode
            yield analytic



class AnalyticSubStream(HttpSubStream, SimplecastFectherStream, ABC):
    primary_key=None

    def __init__(self, endpoint:str, keys_dict:dict, collection_name:str, **kwargs):
        super().__init__(**kwargs)
        self.endpoint=endpoint
        self.keys_dict=keys_dict
        self.collection_name = collection_name

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"analytics/{self.endpoint}?podcast={podcast_id}"

    """
    Default implementation of the parse_response to get the data from the json_objection collection_name.
    If the object mapping is not a simple key mapping then this function as to be overwriten.
    """
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        for elt in data.get(self.collection_name):
            logger.debug("Elt %s", elt)
            analytic={ key: elt.get(key) for key in self.keys_dict }
            yield analytic

class PodcastListeningLocation(AnalyticSubStream):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(endpoint="location", keys_dict=LOCATION_KEYS, collection_name="countries", **kwargs)

class PodcastListeningDevice(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="technology/device_class", keys_dict=TECH_KEY, collection_name="collection", **kwargs)

class PodcastListeningMethod(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="technology/listening_methods", keys_dict=TECH_KEY, collection_name="collection", **kwargs)

# Source
class SourceSimplecastFecther(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"])
        podcasts=Podcast(authenticator=auth)
        episodes=Episode(authenticator=auth, parent=podcasts)
        return [
            podcasts,
            PodcastListeningLocation(authenticator=auth, parent=podcasts),
            PodcastListeningDevice(authenticator=auth, parent=podcasts),
            PodcastListeningMethod(authenticator=auth, parent=podcasts),
            episodes,
            EpisodeDownload(authenticator=auth,parent=episodes, sync_mode=config.get("full_download_sync", False))
        ]
