#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
import time
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")


LOCATION_KEYS=["id", "rank", "name", "downloads_total", "downloads_percent"]
TIME_OF_WEEK_KEYS=["rank", "hour_of_week", "hour_of_day", "day_of_week", "count"]
DOWNLOADS_KEY=["interval", "downloads_total", "downloads_percent"]
TECH_KEY=["rank", "name", "downloads_total", "downloads_percent"]
# Basic full refresh stream
class SimplecastFectherStream(HttpStream):
    url_base = "https://api.simplecast.com/"
    primary_key = None


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        pages =  response.json().get('pages')
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
            return  next_page_token


class Podcast(SimplecastFectherStream):

    primary_key = "podcast_id"


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
    primary_key="episode_id"

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
                "season": elt.get('season'),
                "number": elt.get("number"),
                "description": elt.get("description"),
                "token": elt.get("token"),
                "type": elt.get("type")
            }
            yield episode

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
            analytic={ key: elt.get(key)  for key in self.keys_dict }
            yield analytic

class AnalyticLocation(AnalyticSubStream):
    primary_key="analytic_location_id"

    def __init__(self, **kwargs):
        super().__init__(endpoint="location", keys_dict=LOCATION_KEYS, collection_name="countries", **kwargs)

class AnalyticTimeOfWeek(AnalyticSubStream):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(endpoint="time_of_week", keys_dict=TIME_OF_WEEK_KEYS, collection_name="collection", **kwargs)

class AnalyticEpisode(AnalyticSubStream):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(endpoint="episodes", keys_dict=[], collection_name="", **kwargs)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        for elt in data.get('collection'):
            analytic_episode={
                "id": elt.get("id"),
                "type": elt.get("type"),
                "title": elt.get("title"),
                "downloads": elt.get("downloads").get("total"),
                "number": elt.get("number")
            }
            yield analytic_episode

class AnalyticDownload(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="downloads", keys_dict=DOWNLOADS_KEY, collection_name="by_interval", **kwargs)

class TechnologyApplication(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="technology/applications", keys_dict=TECH_KEY, collection_name="collection", **kwargs)

class TechnologyDeviceClass(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="technology/devices", keys_dict=TECH_KEY, collection_name="collection", **kwargs)

class TechnologyListeningMethod(AnalyticSubStream):

    def __init__(self, **kwargs):
        super().__init__(endpoint="technology/listening_methods", keys_dict=TECH_KEY, collection_name="collection", **kwargs)

class AnalyticEpisodeV2(HttpSubStream,SimplecastFectherStream):
    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        episode_id=stream_slice.get("parent").get("id")
        return f"analytics?episode={episode_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        yield data

class AnalyticPodcastV2(HttpSubStream, SimplecastFectherStream):
    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")
        return f"analytics?podcast={podcast_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.debug("Response: %s", data)
        yield data



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
                episodes,
                AnalyticLocation(authenticator=auth, parent=podcasts),
                AnalyticTimeOfWeek(authenticator=auth, parent=podcasts),
                AnalyticEpisode(authenticator=auth, parent=podcasts),
                AnalyticDownload(authenticator=auth,parent=podcasts),
                TechnologyApplication(authenticator=auth, parent=podcasts),
                TechnologyDeviceClass(authenticator=auth, parent=podcasts),
                TechnologyListeningMethod(authenticator=auth, parent=podcasts),
                AnalyticEpisodeV2(authenticator=auth, parent=episodes),
                AnalyticPodcastV2(authenticator=auth, parent=podcasts)
            ]
