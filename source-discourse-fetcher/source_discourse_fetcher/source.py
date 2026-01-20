#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import time
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.models import SyncMode

logger = logging.getLogger("airbyte")

USER_KEYS = [
    "id","name","username","active","created_at","trust_level","title","time_read", "staged","days_visited","posts_read_count","topics_entered","post_count", "email"
    ]

USER_ACTION_KEYS = [
    "id", "action_type", "created_at", "acting_user_id", "acting_username",
    "target_user_id", "target_post_id", "target_topic_id", "target_username",
    "post_number", "topic_title", "slug", "category_id"
    ]

# Action types to filter: LIKE (1), NEW_TOPIC (4), REPLY (5)
ALLOWED_ACTION_TYPES = [1, 4, 5]

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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._next_page = 0  # Instance attribute for pagination

    @property
    def use_cache(self) -> bool:
        return True

    def path(
       self,
       stream_state: Mapping[str, Any] = None,
       stream_slice: Mapping[str, Any] = None,
       next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/admin/users/list/active.json"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        next_page = {"page": self._next_page} if len(response.json()) > 0 else None
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
        self._next_page = self._next_page + 1


class UserAction(HttpSubStream):
    """
    Fetches user actions (likes, topic creations, replies) for all users.
    """
    primary_key = "id"
    url_base = ""

    def __init__(self, api_key: str, api_username: str, url: str, parent: User, **kwargs):
        super().__init__(parent=parent, **kwargs)
        self.api_key = api_key
        self.api_username = api_username
        self.url = url[:-1] if url.endswith("/") else url
        self._current_slice_key = None
        self._offset = 0

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {"Api-Key": f"{self.api_key}", "Api-Username": f"{self.api_username}"}

    def stream_slices(
        self,
        stream_state: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Override stream_slices to iterate over all users AND all action types.
        For each user from parent, yield 3 slices (one per action type).
        """
        # Reset parent's pagination state so we get ALL users
        self.parent._next_page = 0

        user_count = 0
        slice_count = 0
        for parent_slice in super().stream_slices(sync_mode=SyncMode.full_refresh):
            user_count += 1
            username = parent_slice.get('parent', {}).get('username')
            if user_count <= 10 or user_count % 100 == 0:
                logger.info(f"Generating slices for user {user_count}: {username}")

            # For each user, generate a slice for EACH action type
            for action_type in ALLOWED_ACTION_TYPES:
                slice_count += 1
                yield {
                    "parent": parent_slice.get('parent'),
                    "action_type": action_type
                }

        logger.info(f"Finished generating slices: {slice_count} total from {user_count} users")

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"{self.url}/user_actions.json"

    def request_params(self, stream_state, stream_slice=None, next_page_token: Mapping[str, Any] = None):
        username = stream_slice.get('parent', {}).get('username')
        action_type = stream_slice.get('action_type')
        slice_key = f"{username}_{action_type}"

        # Reset offset when switching to a new user/action_type combination
        if slice_key != self._current_slice_key:
            self._current_slice_key = slice_key
            self._offset = 0
            logger.info(f"Requesting user_actions for {username} filter={action_type}")

        params = {
            "username": username,
            "filter": action_type  # Discourse filter only accepts ONE action type
        }
        if next_page_token:
            params.update(next_page_token)
        return params

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """Use the wait_seconds from Discourse rate limit response for optimal backoff."""
        if response.status_code == 429:
            try:
                data = response.json()
                wait_seconds = data.get("extras", {}).get("wait_seconds")
                if wait_seconds:
                    logger.info(f"Rate limited, waiting {wait_seconds} seconds as requested by API")
                    return float(wait_seconds) + 1
            except Exception:
                pass
        return None  # Fall back to default exponential backoff

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        user_actions = data.get("user_actions", [])
        if len(user_actions) > 0:
            self._offset += len(user_actions)
            return {"offset": self._offset}
        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        data = response.json()
        logger.debug("Response user_actions for %s (filter=%s): %s",
                     stream_slice.get('parent', {}).get('username'),
                     stream_slice.get('action_type'),
                     data)
        for elt in data.get("user_actions", []):
            user_action = {key: elt.get(key) for key in USER_ACTION_KEYS}
            yield user_action

        # Proactive rate limiting: wait 1.5s between requests to avoid hitting
        # Discourse's rate limit (~60 req/min).
        time.sleep(1.5)


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
            post["base_url"] = self.url
            post["post_url"] = self.url + post["post_url"]
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
        user = User(**args)
        # Create a SEPARATE User instance for UserActions to use as parent
        # This prevents pagination state conflicts with the main user stream
        user_for_actions = User(**args)
        group = Group(**args)
        return [
            user,
            UserAction(parent=user_for_actions, **args),
            Post(**args),
            Topic(**args),
            group,
            GroupMember(parent=group, **args),
            Tag(**args),
            Category(**args)
        ]
