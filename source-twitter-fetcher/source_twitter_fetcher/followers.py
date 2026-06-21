from typing import Any, Iterable, Mapping, Optional, List, Union, MutableMapping
import requests
from datetime import datetime
from .tweets_stream import TwitterStream

class AccountFollowers(TwitterStream):

    primary_key = None

    def __init__(self, start_time: Union[str, datetime, None] = None, account_ids: List[str] = [], **kwargs):
        super().__init__(start_time, account_ids, **kwargs)

    def request_params(self, next_page_token: Optional[Mapping[str, Any]] = None, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None) -> MutableMapping[str, Any]:
        params = {
            "max_results": 1000,
        }

        if next_page_token:
            params.update(next_page_token)

        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        output: dict = response.json()
        meta: dict = output.get("meta", {})
        next_token: Optional[str] = meta.get("next_token")
        params = {"pagination_token": next_token} if next_token else None
        return params

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for account in self.account_ids:
            yield {"account_id": account}

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        return f"users/{stream_slice['account_id']}/followers"

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        output: dict = response.json()
        data: list[dict] = output.get("data", [])

        for row in data:
            point = {
                "account_id": str(stream_slice["account_id"]),
                **{f"follower_{key}": value for key, value in row.items()},
                "url": f"https://x.com/{row['username']}"
            }
            yield point

        self._apply_rate_limiting()
