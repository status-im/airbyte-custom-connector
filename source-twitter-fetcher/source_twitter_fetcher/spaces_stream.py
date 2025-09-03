from typing import Any, Iterable, Mapping, MutableMapping, Optional
import logging
import json
from .tweets_stream import TwitterStream

logger = logging.getLogger("airbyte")

class Space(TwitterStream):
    primary_key = "id"

    def __init__(self, space_ids: list = None, **kwargs):
        super().__init__(**kwargs)
        self.space_ids = space_ids or []

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for space_id in self.space_ids:
            yield {"space_id": space_id}

    @property
    def supports_incremental(self) -> bool:
        return False

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"spaces/{stream_slice['space_id']}"

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "space.fields": "id,state,created_at,ended_at,host_ids,lang,is_ticketed,participant_count,speaker_ids,started_at,subscriber_count,title,topic_ids,updated_at,creator_id,invited_user_ids,scheduled_start",
            "expansions": "invited_user_ids,speaker_ids,creator_id,host_ids,topic_ids",
            "user.fields": "id,name,username,created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,verified_type,withheld",
            "topic.fields": "id,name,description"
        }

    def parse_response(
        self,
        response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        try:
            data = response.json()
            
            if 'errors' in data:
                logger.warning(f"Spaces API errors: {data['errors']}")
            
            if 'data' in data and data['data']:
                space = data['data']
                space['space_id'] = stream_slice['space_id']
                
                # Convert arrays to JSON strings
                for field in ['topics', 'host_ids', 'invited_user_ids', 'speaker_ids']:
                    if field in space and isinstance(space[field], list):
                        space[field] = json.dumps(space[field])
                
                # Add expanded data as JSON strings
                if 'includes' in data:
                    includes = data['includes']
                    for key in ['users', 'topics', 'tweets', 'media', 'places', 'polls']:
                        if key in includes:
                            space[f'expanded_{key}'] = json.dumps(includes[key])
                
                yield space
                
        except Exception as e:
            logger.error(f"Error parsing spaces response: {e}")
        
        self._apply_rate_limiting()


class GetSpaceIds(TwitterStream):
    primary_key = "id"
    cursor_field = "created_at"

    @property
    def name(self) -> str:
        return "get_space_ids"

    @property
    def supports_incremental(self) -> bool:
        return True

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        latest_cursor_value = latest_record.get(self.cursor_field)
        current_state_value = current_stream_state.get(self.cursor_field)
        
        if latest_cursor_value and (not current_state_value or latest_cursor_value > current_state_value):
            return {self.cursor_field: latest_cursor_value}
        
        return current_stream_state

    def __init__(self, space_account: list = None, **kwargs):
        super().__init__(**kwargs)
        self.space_account = space_account or []

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        if not self.space_account:
            return
            
        yield {"user_ids": self.space_account, "stream_state": stream_state or {}}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "spaces/by/creator_ids"

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        user_ids = stream_slice.get("user_ids", [])
        return {
            "user_ids": ",".join(user_ids),
            "space.fields": "id,state,created_at,ended_at,host_ids,lang,is_ticketed,participant_count,speaker_ids,started_at,subscriber_count,title,topic_ids,updated_at,creator_id,invited_user_ids,scheduled_start",
            "expansions": "invited_user_ids,speaker_ids,creator_id,host_ids,topic_ids",
            "user.fields": "id,name,username,created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,verified_type,withheld",
            "topic.fields": "id,name,description"
        }

    def parse_response(
        self,
        response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        try:
            data = response.json()
            
            if 'errors' in data:
                logger.warning(f"Spaces by creator IDs API errors: {data['errors']}")
            
            if 'data' in data and data['data']:
                stream_state = stream_slice.get("stream_state", {})
                last_cursor_value = stream_state.get(self.cursor_field)
                
                for space in data['data']:
                    created_at = space.get('created_at')
                    if last_cursor_value is None or (created_at and created_at > last_cursor_value):
                        for field in ['topics', 'host_ids', 'invited_user_ids', 'speaker_ids']:
                            if field in space and isinstance(space[field], list):
                                space[field] = json.dumps(space[field])
                        
                        if 'includes' in data:
                            includes = data['includes']
                            for key in ['users', 'topics', 'tweets', 'media', 'places', 'polls']:
                                if key in includes:
                                    space[f'expanded_{key}'] = json.dumps(includes[key])
                        
                        yield space
                    
        except Exception as e:
            logger.error(f"Error parsing spaces by creator IDs response: {e}")
        
        self._apply_rate_limiting()
