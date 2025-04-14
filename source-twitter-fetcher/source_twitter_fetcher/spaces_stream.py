from typing import Any, Iterable, Mapping, MutableMapping, Optional
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from .tweets_stream import TwitterStream

logger = logging.getLogger("airbyte")

class Space(TwitterStream):
    primary_key = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"spaces/by/creator_ids?user_ids={self.account_id}"

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {
            "space.fields": "created_at,creator_id,ended_at,host_ids,id,invited_user_ids,is_ticketed,lang,participant_count,scheduled_start,speaker_ids,started_at,state,subscriber_count,title,topic_ids,updated_at",
            "topic.fields": "id,description,name",
        }
        return params


    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info("Response: %s", response.json())
        if 'data' in response.json():
            data = response.json()['data']
            for space in data:
                yield space
