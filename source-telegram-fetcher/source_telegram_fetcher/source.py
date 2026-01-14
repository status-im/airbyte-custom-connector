"""Telegram Bot API Airbyte Source Connector."""

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")


class TelegramStream(HttpStream):
    """Base stream for Telegram Bot API."""

    url_base = "https://api.telegram.org/"

    def __init__(self, bot_token: str, **kwargs):
        super().__init__(**kwargs)
        self.bot_token = bot_token

    @property
    def url_base(self) -> str:
        return f"https://api.telegram.org/bot{self.bot_token}/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        return {}


class Messages(TelegramStream):
    """
    Stream for fetching Telegram messages via getUpdates.

    Telegram only retains unacknowledged updates for ~24 hours.
    Once fetched with an offset, updates are marked as acknowledged and removed.

    This stream uses timeout=0 (instant response, not waiting for more updates).
    """

    primary_key = "update_id"
    cursor_field = "update_id"

    def __init__(self, bot_token: str, chat_ids: List[str] = None, **kwargs):
        super().__init__(bot_token=bot_token, **kwargs)
        self.chat_ids = chat_ids or []

    def supports_incremental(self) -> bool:
        return True

    @property
    def source_defined_cursor(self) -> bool:
        return True

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "getUpdates"

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        params = {
            "timeout": 0,  # immediate response
            "allowed_updates": '["message", "channel_post"]', # we don't care about edits here
        }

        # Use offset from state to get only new messages
        if next_page_token and "offset" in next_page_token:
            params["offset"] = next_page_token["offset"]
            logger.info(f"Using offset from next_page_token: {next_page_token['offset']}")
        elif stream_state and "offset" in stream_state:
            params["offset"] = stream_state["offset"]
            logger.info(f"Using offset from stream_state: {stream_state['offset']}")
        else:
            logger.info("No offset found - fetching from beginning")

        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """Telegram returns all available updates at once, no pagination needed."""
        return None

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Update the state with the highest update_id seen.
        The offset should be set to max_update_id + 1 for the next request
        to acknowledge all updates up to that point.
        """
        latest_update_id = latest_record.get("update_id")
        current_offset = current_stream_state.get("offset", 0)

        if latest_update_id is not None:
            # Offset should be max_update_id + 1 to acknowledge all updates
            # Keep the maximum offset to handle multiple records being processed
            new_offset = latest_update_id + 1
            current_stream_state["offset"] = max(new_offset, current_offset)
            logger.info(f"Updated state: offset={current_stream_state['offset']} (from update_id={latest_update_id})")

        return current_stream_state

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        data = response.json()

        if not data.get("ok"):
            logger.error("Telegram API error: %s", data)
            return

        results = data.get("result", [])
        logger.info(f"Fetched {len(results)} updates from Telegram")

        if results:
            update_ids = [u.get("update_id") for u in results]
            logger.info(f"Update IDs in response: {update_ids}")

        for update in results:
            message = update.get("message") or update.get("channel_post")

            if not message:
                continue

            chat = message.get("chat", {})
            chat_id = str(chat.get("id", ""))

            if self.chat_ids and chat_id not in self.chat_ids:
                continue

            enriched_update = {
                "update_id": update["update_id"],
                "update_type": self._get_update_type(update),
                "message_id": message.get("message_id"),
                "date": message.get("date"),
                "text": message.get("text"),
                "caption": message.get("caption"),
                "chat_id": chat.get("id"),
                "chat_title": chat.get("title"),
                "chat_type": chat.get("type"),
                "chat_username": chat.get("username"),
                "is_forum": chat.get("is_forum", False),
                "from_id": message.get("from", {}).get("id"),
                "from_username": message.get("from", {}).get("username"),
                "from_first_name": message.get("from", {}).get("first_name"),
                "from_last_name": message.get("from", {}).get("last_name"),
                "from_is_bot": message.get("from", {}).get("is_bot", False),
                "message_thread_id": message.get("message_thread_id"),
                "is_topic_message": message.get("is_topic_message", False),
                "reply_to_message_id": message.get("reply_to_message", {}).get("message_id"),
                "forward_from_chat_id": message.get("forward_from_chat", {}).get("id"),
                "forward_date": message.get("forward_date"),
                "has_photo": bool(message.get("photo")),
                "has_video": bool(message.get("video")),
                "has_document": bool(message.get("document")),
                "has_audio": bool(message.get("audio")),
                "has_voice": bool(message.get("voice")),
                "has_sticker": bool(message.get("sticker")),
                "has_poll": bool(message.get("poll")),
                "has_location": bool(message.get("location")),
                "raw_update": update,
            }

            yield enriched_update

    def _get_update_type(self, update: Mapping[str, Any]) -> str:
        if "message" in update:
            return "message"
        elif "channel_post" in update:
            return "channel_post"
        return "unknown"

    @property
    def state_checkpoint_interval(self) -> int:
        # Save state after each record to ensure updates are acknowledged immediately
        # This is critical for Telegram API - once fetched, updates must be acknowledged
        # to prevent them from being returned again in the next sync
        return 1


class ChatInfo(TelegramStream):
    """
    Stream for fetching information about configured chats.
    Includes member count and chat details.
    """

    primary_key = "id"

    def __init__(self, bot_token: str, chat_ids: List[str], **kwargs):
        super().__init__(bot_token=bot_token, **kwargs)
        self.chat_ids = chat_ids

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> str:
        return "getChat"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for chat_id in self.chat_ids:
            yield {"chat_id": chat_id}

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        return {"chat_id": stream_slice["chat_id"]}

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        data = response.json()

        if not data.get("ok"):
            logger.warning("Failed to get chat info for %s: %s", stream_slice["chat_id"], data)
            return

        chat = data.get("result", {})

        # Get member count in a separate request
        member_count = self._get_member_count(stream_slice["chat_id"])

        chat_info = {
            "id": chat.get("id"),
            "type": chat.get("type"),
            "title": chat.get("title"),
            "username": chat.get("username"),
            "first_name": chat.get("first_name"),
            "last_name": chat.get("last_name"),
            "is_forum": chat.get("is_forum", False),
            "description": chat.get("description"),
            "invite_link": chat.get("invite_link"),
            "linked_chat_id": chat.get("linked_chat_id"),
            "member_count": member_count,
        }

        yield chat_info

    def _get_member_count(self, chat_id: str) -> Optional[int]:
        """Fetch member count for a chat."""
        try:
            url = f"https://api.telegram.org/bot{self.bot_token}/getChatMemberCount"
            response = requests.get(url, params={"chat_id": chat_id})
            data = response.json()
            if data.get("ok"):
                return data.get("result")
        except Exception as e:
            logger.warning("Failed to get member count for %s: %s", chat_id, e)
        return None


class SourceTelegramFetcher(AbstractSource):

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            bot_token = config["bot_token"]
            url = f"https://api.telegram.org/bot{bot_token}/getMe"
            response = requests.get(url)
            data = response.json()

            if data.get("ok"):
                bot_info = data.get("result", {})
                logger.info(
                    "Connected to Telegram as @%s (%s)",
                    bot_info.get("username"),
                    bot_info.get("first_name"),
                )
                return True, None
            else:
                return False, f"Telegram API error: {data.get('description', 'Unknown error')}"

        except requests.RequestException as e:
            return False, f"Connection error: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        bot_token = config["bot_token"]
        chat_ids = config.get("chat_ids", [])

        streams = [
            Messages(bot_token=bot_token, chat_ids=chat_ids),
        ]
        if chat_ids:
            streams.append(ChatInfo(bot_token=bot_token, chat_ids=chat_ids))

        return streams
