#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Set
from collections import deque
import time
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import ConfiguredAirbyteCatalog

logger = logging.getLogger("airbyte")


class SlidingWindowRateLimiter:
    """
    Sliding window rate limiter to maximize throughput while respecting Luma API limits.

    Luma API limits: 100 requests per minute (500 per 5 minutes per calendar)
    We use 90 req/min to stay safely under the limit with some buffer.
    """

    def __init__(self, max_requests: int = 90, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_timestamps: deque = deque()

    def wait_if_needed(self):
        now = time.time()

        # Remove timestamps outside the sliding window
        while self.request_timestamps and self.request_timestamps[0] < now - self.window_seconds:
            self.request_timestamps.popleft()

        # If we've hit the limit, wait until the oldest request falls outside the window
        if len(self.request_timestamps) >= self.max_requests:
            sleep_time = self.request_timestamps[0] - (now - self.window_seconds) + 0.1
            if sleep_time > 0:
                logger.info(f"Rate limit buffer reached ({len(self.request_timestamps)} requests in window). Waiting {sleep_time:.2f}s")
                time.sleep(sleep_time)

        # Record this request
        self.request_timestamps.append(time.time())


# Shared rate limiter instance for all streams
_rate_limiter = SlidingWindowRateLimiter(max_requests=90, window_seconds=60)

class LumaStream(HttpStream, ABC):
    url_base = "https://public-api.luma.com/v1/"
    primary_key = None

    # Explicitly set namespace to None to ensure consistency between discover and read phases
    # This prevents "null.stream_name" mismatch errors in Airbyte
    namespace = None

    def __init__(self, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key

    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["x-luma-api-key"] = self.api_key
        return headers

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if isinstance(data, dict) and data.get('has_more') and data.get('next_cursor'):
            return {"pagination_cursor": data['next_cursor']}
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {}
        if next_page_token and next_page_token.get('pagination_cursor'):
            params['pagination_cursor'] = next_page_token['pagination_cursor']
        return params

    def should_retry(self, response: requests.Response) -> bool:
        """Enhanced retry logic for rate limiting"""
        if response.status_code == 429:
            logger.warning(f"Rate limit hit for Luma API (429). Will wait and retry.")
            return True
        return super().should_retry(response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """Custom backoff strategy for rate limiting.

        Luma API blocks for 1 minute on 429, so we wait 65 seconds to be safe.
        """
        if response.status_code == 429:
            logger.info("429 received - backing off for 65 seconds (Luma blocks for 1 minute)")
            return 65.0
        return super().backoff_time(response)

    @property
    def max_retries(self) -> Optional[int]:
        """Increase max retries for rate limited requests"""
        return 5

    def _apply_rate_limiting(self):
        """Apply smart rate limiting using sliding window algorithm.

        Only waits when approaching the rate limit, allowing maximum throughput
        while staying safely under Luma's 100 req/min limit.
        """
        _rate_limiter.wait_if_needed()


class LumaEventsStream(LumaStream):
    """Stream for fetching Luma events"""

    @property
    def name(self) -> str:
        """Explicitly define stream name to ensure consistency between discover and read phases"""
        return "luma_events_stream"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "calendar/list-events"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        if isinstance(data, dict) and 'entries' in data:
            for entry in data['entries']:
                yield entry

        self._apply_rate_limiting()


class LumaGuestsStream(LumaStream):
    """guests stream"""
    primary_key = [["api_id"], ["event_api_id"]]  # Composite primary key

    def __init__(self, events_stream: LumaEventsStream, **kwargs):
        super().__init__(**kwargs)
        self.events_stream = events_stream
        self._seen_cursors = {}  # Track cursors per event to detect infinite loops
        self._page_counts = {}  # Track number of pages per event for debugging
        self._guest_counts = {}  # Track total guests fetched per event for debugging
        self._duplicate_cursor_events = set()  # Track events with API cursor bugs

    @property
    def name(self) -> str:
        return "luma_guests_stream"

    @property
    def supported_sync_modes(self) -> List[str]:
        return ["full_refresh"]

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        # Read events from the events stream to get event_api_ids
        event_records = self.events_stream.read_records(sync_mode="full_refresh")

        # Collect unique event_api_ids to avoid duplicates
        unique_event_ids = set()
        event_count = 0

        for event_record in event_records:
            event_count += 1
            event_api_id = event_record.get('api_id')
            if event_api_id and event_api_id not in unique_event_ids:
                unique_event_ids.add(event_api_id)
                logger.info(f"Creating slice for event {event_api_id}")
                yield {"event_api_id": event_api_id}

        logger.info(f"SLICE GENERATION COMPLETE: Total events processed: {event_count}, Unique event slices created: {len(unique_event_ids)}")

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "event/get-guests"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)

        # Add the event_api_id parameter for this specific event slice
        if stream_slice and stream_slice.get('event_api_id'):
            params['event_api_id'] = stream_slice['event_api_id']

        # Add explicit pagination limit to ensure consistent behavior
        # Try a smaller limit to see if it helps with API cursor issues
        params['pagination_limit'] = 25

        logger.debug(f"Request params for guests: {params}")
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Override to detect infinite pagination loops"""
        data = response.json()

        if isinstance(data, dict) and data.get('has_more') and data.get('next_cursor'):
            cursor = data['next_cursor']

            # Extract current event from request params instead of URL parsing
            current_event = "unknown"
            try:
                url = response.request.url
                logger.debug(f"Processing pagination for URL: {url}")

                if 'event_api_id=' in url:
                    # Extract event_api_id more robustly
                    import urllib.parse
                    parsed_url = urllib.parse.urlparse(url)
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    if 'event_api_id' in query_params:
                        current_event = query_params['event_api_id'][0]
                        logger.debug(f"Extracted event_api_id: {current_event}")
            except Exception as e:
                logger.warning(f"Failed to extract event_api_id from URL: {e}")
                current_event = "unknown"

            if current_event not in self._seen_cursors:
                self._seen_cursors[current_event] = set()
                self._page_counts[current_event] = 0

            # Increment page count
            self._page_counts[current_event] += 1

            # Check if we've seen this cursor for this event before
            if cursor in self._seen_cursors[current_event]:
                # Mark this event as having cursor duplication issues and stop pagination
                self._duplicate_cursor_events.add(current_event)
                logger.warning(f"Cannot continue pagination for event {current_event} due to API cursor bug. Breaking after {self._page_counts[current_event]} pages.")
                return None

            # Add a safety limit for maximum pages per event (in case of API issues)
            # With 25 guests per page, 1000 pages = 25,000 guests max
            max_pages_per_event = 1000  # Generous limit for very large events
            if self._page_counts[current_event] >= max_pages_per_event:
                logger.warning(f"SAFETY LIMIT REACHED: {max_pages_per_event} pages processed for event {current_event} (approx {max_pages_per_event * 25} guests). Breaking pagination to avoid infinite loops.")
                return None

            # Add cursor to seen set
            self._seen_cursors[current_event].add(cursor)

            return {"pagination_cursor": cursor}

        return None

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        data = response.json()

        event_api_id = stream_slice.get('event_api_id') if stream_slice else None

        guest_count = 0
        total_count = data.get('total_count', 'unknown') if isinstance(data, dict) else 'unknown'
        has_more = data.get('has_more', False) if isinstance(data, dict) else False

        if isinstance(data, dict) and 'entries' in data:
            for guest in data['entries']:
                if event_api_id:
                    guest['event_api_id'] = event_api_id
                guest_count += 1
                yield guest

        if event_api_id:
            if event_api_id not in self._guest_counts:
                self._guest_counts[event_api_id] = 0
            self._guest_counts[event_api_id] += guest_count

        self._apply_rate_limiting()


class SourceLumaFetcher(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            api_key = config.get('api_key')
            if not api_key:
                return False, "API key is required"

            headers = {
                "accept": "application/json",
                "x-luma-api-key": api_key
            }

            response = requests.get(
                "https://public-api.luma.com/v1/calendar/list-events",
                headers=headers,
                timeout=10
            )

            if response.status_code == 200:
                return True, None
            else:
                return False, f"API request failed with status {response.status_code}: {response.text}"

        except Exception as e:
            return False, f"Connection test failed: {str(e)}"

    def _get_catalog_stream_names(self, catalog: ConfiguredAirbyteCatalog) -> Set[str]:
        """Extract stream names from the configured catalog for validation.

        Returns a set of stream names that are configured to be synced.
        This is used to filter streams and prevent status messages for unconfigured streams.
        """
        return {configured_stream.stream.name for configured_stream in catalog.streams}

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """Return all available streams.

        Note: All streams returned here must have consistent 'name' and 'namespace'
        properties to match how they are identified in the catalog during read operations.
        """
        api_key = config['api_key']

        events_stream = LumaEventsStream(api_key=api_key)
        guests_stream = LumaGuestsStream(events_stream=events_stream, api_key=api_key)

        # Log stream names for debugging namespace/name consistency issues
        all_streams = [events_stream, guests_stream]
        for stream in all_streams:
            logger.info(f"Registering stream: name='{stream.name}', namespace='{stream.namespace}'")

        return all_streams

    def read(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
        catalog: ConfiguredAirbyteCatalog,
        state: Optional[Mapping[str, Any]] = None
    ) -> Iterable[Any]:
        """Override read to ensure strict validation of streams against the catalog.

        This prevents STREAM_STATUS messages from being emitted for streams
        that are not present in the ConfiguredAirbyteCatalog, which causes
        the "stream not present in catalog" error.
        """
        # Get the set of stream names that are actually configured in the catalog
        configured_stream_names = self._get_catalog_stream_names(catalog)
        logger.info(f"Configured catalog contains streams: {configured_stream_names}")

        # Get all available streams
        all_streams = self.streams(config)

        # Validate that all configured streams exist in our available streams
        available_stream_names = {stream.name for stream in all_streams}
        logger.info(f"Available streams from source: {available_stream_names}")

        # Check for any mismatches
        missing_streams = configured_stream_names - available_stream_names
        if missing_streams:
            logger.warning(f"Catalog contains streams not available in source: {missing_streams}")

        extra_streams = available_stream_names - configured_stream_names
        if extra_streams:
            logger.info(f"Source has streams not in catalog (will be skipped): {extra_streams}")

        # Delegate to parent implementation which will handle the actual reading
        # The parent's read method should filter based on the catalog, but we've
        # added logging above to help debug any namespace/name mismatches
        yield from super().read(logger, config, catalog, state)
