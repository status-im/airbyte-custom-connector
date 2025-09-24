#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream

logger = logging.getLogger("airbyte")

USER_KEYS = [
    "id","name","username","active","created_at","trust_level","title","time_read", "staged","days_visited","posts_read_count","topics_entered","post_count", "email"
    ]

POST_KEYS = [
    "id","name","username", "raw", "created_at", "post_number", "post_type", "post_count", "post_url", "updated_at", "reply_count", "reply_to_post_number","quote_count","incoming_link_count","reads","score","topic_id", "topic_slug","topic_title","topic_html_title","category_id"
    ]

class DiscourseStream(HttpStream):

    url_base = ""
    primary_key = None

    def __init__(self, api_key: str, api_username: str, url: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.api_username = api_username
        self.url= url[:-1] if url.endswith("/") else url

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return { "Api-Key" : f"{self.api_key}", "Api-Username": f"{self.api_username}"}

class User(DiscourseStream):
    primary_key="id"
    next_page = 0
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/admin/users/list/active.json"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page = {"page": self.next_page } if len(response.json()) > 0 else None
        return next_page


    def request_params(self, stream_state, stream_slice = None, next_page_token = None):
        params = {
            "order": "created",
            "asc": "true"
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(
       self,
       response: requests.Response,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        for elt in data:
            logger.debug("Response %s", elt)
            user = { key : elt.get(key) for key in USER_KEYS }
            yield user
        self.next_page = self.next_page + 1

class Post(DiscourseStream):
    primary_key="id"
    cursor = "created_at"
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/posts.json"

    def parse_response(
       self,
       response: requests.Response,
       next_page_token: Mapping[str, Any] = None,
       **kwargs
    ) -> Iterable[Mapping]:
        data: dict = response.json()
        for elt in data.get("latest_posts"):
            post = { key : elt.get(key) for key in POST_KEYS }
            # Make RAG project assignment easier
            post["base_url"] = self.url
            post["post_url"] = self.url + post["post_url"] # post_url always starts with '/'
            yield post

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        posts: list[dict] = response.json().get("latest_posts", [])
        next_page = {"before": posts[-1]["id"]} if posts else None
        return next_page


    def request_params(self, stream_state, stream_slice = None, next_page_token: Mapping[str, Any] = None):
        params = {}
        if next_page_token:
            params.update(next_page_token)
        return params

class Topic(DiscourseStream):
    primary_key="id"
    next_page = 0
    # https://docs.discourse.org/#tag/Topics/operation/listLatestTopics
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/latest.json"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        topics_list=response.json().get("topic_list").get("topics")
        next_page = {"page": self.next_page } if len(topics_list) > 0 else None
        return next_page


    def request_params(self, stream_state, stream_slice = None, next_page_token: Mapping[str, Any] = None):
        params = {
            "order": "created",
            "ascending": "true",
            "per_page": "100"
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(
       self,
       response: requests.Response,
        next_page_token: Mapping[str, Any] = None,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response latest topics %s", data)
        for elt in data.get("topic_list").get("topics"):
            yield elt
        self.next_page = self.next_page + 1


class Group(DiscourseStream):
    primary_key="id"
    use_cache=True

    # https://docs.discourse.org/#tag/Groups/operation/listGroups
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/groups.json"

    def parse_response(
       self,
       response: requests.Response,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response groups %s", data)
        for elt in data.get("groups"):
            yield elt

class GroupMember(HttpSubStream, Group):
    primary_key="id"
    # https://docs.discourse.org/#tag/Groups/operation/listGroupMembers
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        group_id = stream_slice.get('parent').get('name')
        return f"{self.url}/groups/{group_id}/members.json"

    def parse_response(
       self,
       response: requests.Response,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        for elt in data.get("members"):
            yield elt


class Tag(DiscourseStream):
    primary_key="id"
    # https://docs.discourse.org/#tag/Topics/operation/listLatestTopics
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/tags.json"

    def parse_response(
       self,
       response: requests.Response,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response groups %s", data)
        for elt in data.get("tags"):
            yield elt


class Category(DiscourseStream):
    primary_key="id"
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/categories.json"

    def parse_response(
       self,
       response: requests.Response,
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response groups %s", data)
        for elt in data.get("category_list").get("categories"):
            yield elt




# Source
class SourceDiscourseFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        logger.info("Configuring Stream fron %s", config["url"])
        args = {
            "api_key": config['api-key'],
            "api_username": config['api-username'],
            "url": config['url']
        }
        group=Group(**args)
        return [
            User(**args),
            Post(**args),
            Topic(**args),
            group,
            GroupMember(parent=group, **args),
            Tag(**args),
            Category(**args)
        ]
