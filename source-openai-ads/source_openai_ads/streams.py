import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream

logger = logging.getLogger("airbyte")

ATTRIBUTION_WINDOW_DAYS = 30
VIEW_THROUGH_WINDOW_DAYS = 1
ATTRIBUTION_TIME_BASIS = "ad_event_time"
REPORT_DAYS = 7

DELIVERY_METRICS = ("impressions", "clicks", "spend", "ctr", "cpc", "cpm")
CONVERSION_METRICS = (
    "conversions",
    "cpa",
    "post_click_cvr",
    "order_created_attributed_sales",
    "order_created_attributed_sales_currency",
    "order_created_roas",
)
INSIGHT_FIELDS = ["campaign_id", "campaign_name", "readable_time", *DELIVERY_METRICS, *CONVERSION_METRICS]


def pick(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def metric(row: Mapping[str, Any], name: str, prefix: Optional[str] = None) -> Any:
    if prefix:
        return pick(row, f"{prefix}.{name}", f"{prefix}_{name}", name)
    return pick(row, name, f"campaign.{name}")


def apply_metrics(record: MutableMapping[str, Any], item: Mapping[str, Any], names, prefix=None) -> None:
    for name in names:
        record[name] = metric(item, name, prefix)


def dimension(row: Mapping[str, Any], segment: str) -> Any:
    keys = {
        "country": ("country_name", "country.name", "country"),
        "device": ("device_type", "device.type", "device"),
        "platform": ("platform",),
    }[segment]
    value = pick(row, *keys)
    if isinstance(value, dict):
        return value.get("name") or value.get("type")
    return value


def attribution() -> Mapping[str, Any]:
    return {
        "attribution_window_days": ATTRIBUTION_WINDOW_DAYS,
        "view_through_attribution_window_days": VIEW_THROUGH_WINDOW_DAYS,
        "attribution_time_basis": ATTRIBUTION_TIME_BASIS,
    }


def time_range(since: str, until: str, timezone_name: Optional[str]) -> str:
    payload = {"type": "date_range", "since": since, "until": until}
    if timezone_name:
        payload["timezone"] = timezone_name
    return json.dumps(payload, separators=(",", ":"))


def report_bounds(today) -> tuple:
    return (today - timedelta(days=REPORT_DAYS)).isoformat(), today.isoformat()


class OpenAIAdsStream(HttpStream):
    primary_key = "id"

    @property
    def url_base(self) -> str:
        from .source import API_ROOT

        return API_ROOT
    page_size = 500

    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        return {"Accept": "application/json"}

    def request_kwargs(self, **kwargs) -> Mapping[str, Any]:
        return {"timeout": 120}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        try:
            body = response.json()
        except ValueError:
            return None
        if isinstance(body, dict) and body.get("has_more") and body.get("last_id"):
            return {"after": body["last_id"]}
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params: MutableMapping[str, Any] = {"limit": self.page_size}
        if next_page_token and next_page_token.get("after"):
            params["after"] = next_page_token["after"]
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response.raise_for_status()
        body = response.json()
        rows = body if isinstance(body, list) else (body.get("data") or [])
        for row in rows:
            yield row


class AdAccount(OpenAIAdsStream):
    @property
    def name(self) -> str:
        return "ad_account"

    def path(self, **kwargs) -> str:
        return "ad_account"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response.raise_for_status()
        yield response.json()


class Campaigns(OpenAIAdsStream):
    @property
    def name(self) -> str:
        return "campaigns"

    @property
    def use_cache(self) -> bool:
        return True

    def path(self, **kwargs) -> str:
        return "campaigns"


class ChildList(HttpSubStream, OpenAIAdsStream):
    """Rows for one parent id, passed as a query parameter."""

    query_param = ""

    def request_params(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = OpenAIAdsStream.request_params(
            self,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )
        params[self.query_param] = stream_slice["parent"]["id"]
        return params


class AdGroups(ChildList):
    query_param = "campaign_id"

    @property
    def name(self) -> str:
        return "ad_groups"

    @property
    def use_cache(self) -> bool:
        return True

    def path(self, **kwargs) -> str:
        return "ad_groups"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        campaign_id = stream_slice["parent"]["id"]
        for row in OpenAIAdsStream.parse_response(self, response, stream_slice=stream_slice, **kwargs):
            row["campaign_id"] = campaign_id
            yield row


class Ads(ChildList):
    query_param = "ad_group_id"

    @property
    def name(self) -> str:
        return "ads"

    def path(self, **kwargs) -> str:
        return "ads"

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        parent = stream_slice["parent"]
        for row in OpenAIAdsStream.parse_response(self, response, stream_slice=stream_slice, **kwargs):
            row["ad_group_id"] = parent["id"]
            row["campaign_id"] = parent.get("campaign_id")
            yield row


class EventSettings(OpenAIAdsStream):
    @property
    def name(self) -> str:
        return "event_settings"

    def path(self, **kwargs) -> str:
        return "conversions/event_settings"


class DatedReport(OpenAIAdsStream):
    def __init__(self, today, currency_code: Optional[str], timezone_name: Optional[str], **kwargs):
        super().__init__(**kwargs)
        self.since, self.until = report_bounds(today)
        self.currency_code = currency_code
        self.timezone_name = timezone_name


def segment_schema(segment: str) -> Mapping[str, Any]:
    string = {"type": ["null", "string"]}
    number = {"type": ["null", "number"]}
    properties: MutableMapping[str, Any] = {
        "campaign_id": string,
        "campaign_name": string,
        "readable_time": {"type": ["null", "string"], "format": "date"},
        **{name: number for name in DELIVERY_METRICS},
        "currency_code": string,
        "timezone": string,
        **{key: {"type": ["null", "integer"]} for key in (
            "attribution_window_days",
            "view_through_attribution_window_days",
        )},
        "attribution_time_basis": string,
    }
    if segment == "product":
        properties["product_feed_id"] = string
        properties["product_item_id"] = string
        properties["product_title"] = string
    else:
        properties[segment] = string
    return {"type": "object", "additionalProperties": True, "properties": properties}


class CampaignInsights(DatedReport):
    """One row per campaign per day. Pass segment for country, device, platform, or product."""

    primary_key = ["campaign_id", "readable_time"]
    page_size = 2000

    def __init__(self, segment: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.segment = segment
        if segment == "product":
            self.primary_key = ["campaign_id", "readable_time", "product_feed_id", "product_item_id"]
        elif segment:
            self.primary_key = ["campaign_id", "readable_time", segment]

    @property
    def name(self) -> str:
        if self.segment:
            return f"campaign_insights_{self.segment}"
        return "campaign_insights"

    def get_json_schema(self) -> Mapping[str, Any]:
        if self.segment:
            return segment_schema(self.segment)
        return super().get_json_schema()

    def _fields(self) -> List[str]:
        if not self.segment:
            return INSIGHT_FIELDS
        fields = ["campaign_id", "campaign_name", "readable_time"]
        if self.segment == "country":
            fields.append("country.name")
        elif self.segment == "device":
            fields.append("device.type")
        elif self.segment == "platform":
            fields.append("platform")
        else:
            fields.extend(["product.feed_id", "product.item_id", "product.title"])
        fields.extend(f"{self.segment}.{name}" for name in DELIVERY_METRICS)
        return fields

    def path(self, **kwargs) -> str:
        return "ad_account/insights"

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        logger.info("%s from %s to %s", self.name, self.since, self.until)
        yield {"since": self.since, "until": self.until}

    def request_params(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        params = OpenAIAdsStream.request_params(
            self,
            stream_state=stream_state,
            stream_slice=stream_slice,
            next_page_token=next_page_token,
        )
        params.update(
            {
                "aggregation_level": "campaign",
                "time_granularity": "daily",
                **attribution(),
                "time_ranges[]": [time_range(stream_slice["since"], stream_slice["until"], self.timezone_name)],
                "fields[]": self._fields(),
            }
        )
        if self.segment:
            params["segments[]"] = [self.segment]
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for item in OpenAIAdsStream.parse_response(self, response, **kwargs):
            campaign_id = pick(item, "campaign_id", "campaign.id")
            readable_time = pick(item, "readable_time", "metadata.readable_time")
            if not campaign_id or not readable_time:
                logger.warning("Skipping insight row without campaign or date: %s", item.get("id"))
                continue
            record = dict(item)
            record.update(
                {
                    "campaign_id": campaign_id,
                    "campaign_name": pick(item, "campaign_name", "campaign.name"),
                    "readable_time": str(readable_time)[:10],
                    "currency_code": self.currency_code,
                    "timezone": self.timezone_name,
                    **attribution(),
                }
            )
            apply_metrics(record, item, DELIVERY_METRICS, self.segment)
            if not self.segment:
                apply_metrics(record, item, CONVERSION_METRICS)
            elif self.segment == "product":
                feed = pick(item, "product_feed_id", "product.feed_id", "feed_id")
                item_id = pick(item, "product_item_id", "item_id", "product.item_id")
                if not feed or not item_id:
                    logger.warning("Skipping product row without feed and item")
                    continue
                record["product_feed_id"] = feed
                record["product_item_id"] = item_id
                record["product_title"] = pick(item, "product_title", "product.title", "title")
            else:
                value = dimension(item, self.segment)
                if not value:
                    logger.warning("Skipping %s row without a breakdown value", self.segment)
                    continue
                record[self.segment] = value
            yield record


class CampaignConversions(HttpSubStream, OpenAIAdsStream):
    """Click-through vs view-through conversions, plus each attributed event."""

    primary_key = ["campaign_id", "date"]

    def __init__(
        self,
        today,
        currency_code: Optional[str],
        timezone_name: Optional[str],
        breakdown: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.since, self.until = report_bounds(today)
        self.currency_code = currency_code
        self.timezone_name = timezone_name
        self.breakdown = breakdown
        if breakdown:
            self.primary_key = ["campaign_id", "date", breakdown]

    @property
    def name(self) -> str:
        if self.breakdown:
            return f"campaign_conversions_{self.breakdown}"
        return "campaign_conversions"

    def get_json_schema(self) -> Mapping[str, Any]:
        if not self.breakdown:
            return super().get_json_schema()
        path = Path(__file__).parent / "schemas" / "campaign_conversions.json"
        schema = json.loads(path.read_text())
        schema["properties"][self.breakdown] = {"type": ["null", "string"]}
        return schema

    @property
    def http_method(self) -> str:
        return "POST"

    def path(self, **kwargs) -> str:
        return "conversions/insights"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, **kwargs) -> MutableMapping[str, Any]:
        return {}

    def stream_slices(
        self,
        sync_mode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        logger.info("%s from %s to %s", self.name, self.since, self.until)
        parent_slices = HttpSubStream.stream_slices(
            self,
            sync_mode=SyncMode.full_refresh,
            cursor_field=cursor_field,
            stream_state={},
        )
        for parent_slice in parent_slices:
            campaign_id = (parent_slice.get("parent") or {}).get("id")
            if campaign_id:
                yield {"campaign_id": campaign_id, "since": self.since, "until": self.until}

    def request_body_json(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        body = {
            "aggregation_level": "campaign",
            "time_granularity": "daily",
            "time_ranges": [time_range(stream_slice["since"], stream_slice["until"], self.timezone_name)],
            "entity_ids": [stream_slice["campaign_id"]],
            **attribution(),
        }
        if self.breakdown:
            body["breakdown"] = self.breakdown
        else:
            body["include"] = ["attributed_events"]
        return body

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:
        response.raise_for_status()
        body = response.json()
        account_currency = body.get("account_currency") or self.currency_code
        for item in body.get("data") or []:
            campaign_id = item.get("entity_id") or (stream_slice or {}).get("campaign_id")
            day = item.get("date")
            if not campaign_id or not day:
                logger.warning("Skipping conversion row without campaign or date")
                continue
            record = dict(item)
            record.update(
                {
                    "campaign_id": campaign_id,
                    "date": str(day)[:10],
                    "account_currency": account_currency,
                    "timezone": self.timezone_name,
                    **attribution(),
                }
            )
            if self.breakdown:
                value = dimension(item, self.breakdown)
                if not value:
                    logger.warning("Skipping conversion row without %s", self.breakdown)
                    continue
                record[self.breakdown] = value
            yield record
