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

USER_KEYS = [
    "id","name","username","active","created_at","trust_level","title","time_read", "staged","days_visited","posts_read_count","topics_entered","post_count", "email" 
    ]

POST_KEYS = [
    "id","name","username","created_at","post_number","post_type","updated_at","reply_count", "reply_to_post_number","quote_count","incoming_link_count","reads","score","topic_id", "topic_slug","topic_title","topic_html_title","category_id" 
    ]

class DiscourseStream(HttpStream):

    url_base =  ""
    primary_key = None

    def __init__(self, api_key: str, api_username: str, url: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.api_username = api_username
        self.url= url

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return { "Api-Key" : f"{self.api_key}", "Api-Username": f"{self.api_username}"}

class User(DiscourseStream):
    primary_key="id" 

    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/admin/users/list/active.json"

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

class Post(DiscourseStream):
    primary_key="id" 

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
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response %s", data)
        for elt in data.get("latest_posts"):
            post =  { key : elt.get(key) for key in POST_KEYS }
            yield post

class Topic(DiscourseStream):
    primary_key="id"
    # https://docs.discourse.org/#tag/Topics/operation/listLatestTopics
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/latest.json"

    def parse_response(
       self,
       response: requests.Response, 
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response latest topics %s", data)
        for elt in data.get("topic_list").get("topics"):
            yield elt

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

class GroupMember(HttpSubStream, DiscourseStream):
    primary_key="id"
    
    # https://docs.discourse.org/#tag/Groups/operation/listGroupMembers
    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        group_id = stream_slice.get('parent').get('id')
        return f"{self.url}/groups/{group_id}/members.json"

    def parse_response(
       self,
       response: requests.Response, 
       **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response groups %s", data)
        for elt in data.get("groups"):
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
        logger.info("Configuring Stream  fron %s", config["url"])
        group=Group(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url']
            )
        s = [
            User(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url']
            ),
            Post(
               api_key         = config['api-key'],
               api_username    = config['api-username'],
               url             = config['url']
           ),
            Topic(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url']
            ),
            group,
            GroupMember(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url'],
                parent=group
                ),
            Tag(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url']
            ),
            Category(
                api_key         = config['api-key'],
                api_username    = config['api-username'],
                url             = config['url']
            )
        ]
        return s 
