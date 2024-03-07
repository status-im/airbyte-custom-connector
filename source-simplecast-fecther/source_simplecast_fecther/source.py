#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

logger = logging.getLogger("airbyte")


LOCATION_KEYS=["id", "rank", "name", "downloads_total", "downloads_percent"]
TIME_OF_WEEK_KEYS=["rank", "hour_of_week", "hour_of_day", "day_of_week", "count"]
DOWNLOADS_KEY=["interval", "downloads_total", "downloads_percent"]
# Basic full refresh stream
class SimplecastFectherStream(HttpStream):
    url_base = "https://api.simplecast.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

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
        logger.info("Response: %s", data)
        if 'collection' not in data.keys():
            logger.debug("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
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

class Episode(HttpSubStream, Podcast):
    primary_key="episode_id"

    def __init__(self, **kwargs):
        super().__init__(Podcast(**kwargs), **kwargs)

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
        if 'collection' not in data.keys():
            logger.error("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
        for elt in data.get('collection'):
            episode={
                "id": elt.get("id"),
                "title": elt.get("title"),
                "status": elt.get("status"),
                "published_at": elt.get("published_at"),
                "updated_at": elt.get("updated_at"),
                "season_href": elt.get('season').get("href"),
                "season_number": elt.get('season').get("number"),
                "number": elt.get("number"),
                "description": elt.get("description"),
                "token": elt.get("token"),
                "type": elt.get("type")
            }
            yield episode

class AnalyticLocation(HttpSubStream, Podcast):
    primary_key="analytic_location_id"


    def __init__(self, **kwargs):
        super().__init__(Podcast(**kwargs), **kwargs)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"analytics/location?podcast={podcast_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.info("Response: %s", data)
        if 'countries' not in data.keys():
            logger.error("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
        for elt in data.get('countries'):
            location={ key: elt.get(key) for key in LOCATION_KEYS }
            yield location

class AnalyticTimeOfWeek(HttpSubStream, Podcast):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(Podcast(**kwargs), **kwargs)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"analytics/time_of_week?podcast={podcast_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.info("Response: %s", data)
        if 'collection' not in data.keys():
            logger.error("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
        for elt in data.get('collection'):
            time_of_week={ key: elt.get(key) for key in TIME_OF_WEEK_KEYS }
            yield time_of_week

class AnalyticEpisode(HttpSubStream, Podcast):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(Podcast(**kwargs), **kwargs)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"analytics/episodes?podcast={podcast_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.info("Response: %s", data)
        if 'collection' not in data.keys():
            logger.error("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
        for elt in data.get('collection'):
            analytic_episode={
                "id": elt.get("id"),
                "type": elt.get("type"),
                "title": elt.get("title"),
                "downloads": elt.get("downloads").get("total"),
                "number": elt.get("number")
            }
            yield analytic_episode

class AnalyticDownload(HttpSubStream, Podcast):
    primary_key=None

    def __init__(self, **kwargs):
        super().__init__(Podcast(**kwargs), **kwargs)

    def path(
        self, 
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        podcast_id=stream_slice.get("parent").get("id")

        return f"analytics/downloads?podcast={podcast_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data=response.json()
        logger.info("Response: %s", data)
        if 'by_interval' not in data.keys():
            logger.error("Error when trying to get the data %s", data)
            raise Exception("error when calling the api")
        for elt in data.get('by_interval'):
            download={ key: elt.get(key)  for key in DOWNLOADS_KEY }
            yield download

# Source
class SourceSimplecastFecther(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(token=config["api_key"])  
        return [
                Podcast(authenticator=auth),
                Episode(authenticator=auth),
                AnalyticLocation(authenticator=auth),
                AnalyticTimeOfWeek(authenticator=auth),
                AnalyticEpisode(authenticator=auth),
                AnalyticDownload(authenticator=auth)
            ]
