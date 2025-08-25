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
