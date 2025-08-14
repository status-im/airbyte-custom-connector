from typing import Any, Iterable, Mapping, MutableMapping, Optional, Tuple, Union
import logging
import requests
import time
from datetime import datetime, timedelta
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from .tweets_stream import TwitterStream

logger = logging.getLogger("airbyte")

class Space(TwitterStream):
    primary_key = "id"

    def __init__(self, start_time: Union[str, datetime, None] = None, account_id: str = None, **kwargs):
        super().__init__(start_time=start_time, account_id=account_id, **kwargs)
    def __init__(self, account_id: str = None, **kwargs):
        super().__init__(account_id=account_id, **kwargs)
        logger.debug(f"Space stream initialized with account_id: {self.account_id}")

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        """Generate stream slices for spaces sync"""
        logger.info(f"🚀 Space stream_slices called with account_id: {self.account_id}")
        # For spaces, we just need one slice since we're fetching all spaces for the account
        slice_data = {"account_id": self.account_id}
        logger.info(f"📦 Yielding space slice: {slice_data}")
        yield slice_data
        logger.info(f"✅ Space stream_slices completed")

    @property 
    def name(self) -> str:
        """Explicit stream name for Airbyte discovery"""
        return "space"

    @property
    def source_defined_cursor(self) -> bool:
        """This stream doesn't use incremental sync"""
        return False

    @property
    def supports_incremental(self) -> bool:
        """This stream supports full refresh only"""
        return False

    @property
    def use_cache(self) -> bool:
        return True

    def should_retry(self, response: requests.Response) -> bool:
        """Determine if request should be retried"""
        return response.status_code in [429, 500, 502, 503, 504]

    def check_availability(self, logger, source) -> Tuple[bool, Optional[str]]:
        """Check if the stream is available"""
        try:
            # Just verify the stream can be initialized
            logger.info(f"Checking availability for Space stream with account_id: {self.account_id}")
            return True, None
        except Exception as e:
            logger.error(f"Space stream availability check failed: {e}")
            return False, str(e)

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        # Use the endpoint as originally intended
        logger.info(f"🔗 Space stream path called with:")
        logger.info(f"   - account_id: {self.account_id}")
        logger.info(f"   - stream_slice: {stream_slice}")
        logger.info(f"   - next_page_token: {next_page_token}")
        
        path = f"spaces/by/creator_ids?user_ids={self.account_id}"
        logger.info(f"📍 Generated path: {path}")
        return path

    def request_params(
        self,
        next_page_token: Optional[Mapping[str, Any]] = None,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        logger.info(f"⚙️ Space request_params called with:")
        logger.info(f"   - stream_slice: {stream_slice}")
        logger.info(f"   - next_page_token: {next_page_token}")
        
        params = {
            "space.fields": "created_at,creator_id,ended_at,host_ids,id,invited_user_ids,is_ticketed,lang,participant_count,scheduled_start,speaker_ids,started_at,state,subscriber_count,title,topic_ids,updated_at",
            "topic.fields": "id,description,name",
            "expansions": "creator_id,host_ids,speaker_ids,topic_ids"
        }
        
        # Add pagination token if provided
        if next_page_token:
            params.update(next_page_token)
            logger.info(f"🔄 Added pagination token: {next_page_token}")
            
        logger.info(f"📋 Final request params: {params}")
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Handle pagination for Spaces API"""
        try:
            response_json = response.json()
            if 'meta' in response_json and 'next_token' in response_json['meta']:
                return {"next_token": response_json['meta']['next_token']}
        except Exception as e:
            logger.warning(f"Error parsing pagination token: {e}")
        return None

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        logger.info(f"🎯 Space parse_response called!")
        logger.info(f"   - Response status: {response.status_code}")
        logger.info(f"   - Stream slice: {stream_slice}")
        logger.info(f"   - Response headers: {dict(response.headers)}")
        
        try:
            response_data = response.json()
            logger.info(f"📊 Spaces API Response received")
            logger.debug(f"📄 Full response: {response_data}")
            
            # Check for errors in response
            if 'errors' in response_data:
                logger.warning(f"⚠️ Spaces API returned errors: {response_data['errors']}")
                for error in response_data['errors']:
                    logger.error(f"   - Error: {error}")
            
            # Check if there's data
            if 'data' in response_data and response_data['data']:
                data = response_data['data']
                logger.info(f"🎉 Found {len(data)} spaces for account {self.account_id}")
                
                for i, space in enumerate(data):
                    # Add account_id to each space record
                    space['account_id'] = self.account_id
                    logger.info(f"   📦 Yielding space {i+1}: {space.get('id', 'unknown_id')}")
                    yield space
                    
                logger.info(f"✅ Successfully yielded {len(data)} spaces")
            else:
                logger.info(f"📭 No spaces found for account {self.account_id}")
                
                # Check if it's because of permissions or no spaces exist
                if 'meta' in response_data:
                    result_count = response_data['meta'].get('result_count', 0)
                    logger.info(f"📊 API result count: {result_count}")
                    
                # Log the full response when no data is found
                logger.info(f"📄 Empty response details: {response_data}")
                
        except Exception as e:
            logger.error(f"❌ Error parsing spaces response: {e}")
            logger.error(f"📄 Raw response content: {response.text}")
            import traceback
            logger.error(f"🔍 Full traceback: {traceback.format_exc()}")
        
        # Apply rate limiting like other Twitter streams
        logger.info(f"⏰ Applying rate limiting...")
        self._apply_rate_limiting()
        logger.info(f"✅ Space parse_response completed")
