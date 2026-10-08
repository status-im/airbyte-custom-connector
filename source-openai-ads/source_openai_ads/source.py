import logging
from datetime import datetime, timezone
from typing import Any, List, Mapping, Tuple
from zoneinfo import ZoneInfo

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import (
    AdAccount,
    AdGroups,
    Ads,
    CampaignConversions,
    CampaignInsights,
    Campaigns,
    EventSettings,
)

API_ROOT = "https://api.ads.openai.com/v1/"

logger = logging.getLogger("airbyte")


def fetch_account(api_key: str) -> requests.Response:
    return requests.get(
        f"{API_ROOT}ad_account",
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        timeout=60,
    )


def account_today(timezone_name):
    if timezone_name:
        try:
            return datetime.now(ZoneInfo(timezone_name)).date()
        except Exception:
            logger.warning("Unknown account timezone %s; using UTC", timezone_name)
    return datetime.now(timezone.utc).date()


class SourceOpenAIAds(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        api_key = config.get("api_key")
        if not api_key:
            return False, "api_key is required"
        response = fetch_account(api_key)
        if response.status_code != 200:
            return False, f"GET /ad_account returned {response.status_code}: {response.text[:300]}"
        if not response.json().get("id"):
            return False, "GET /ad_account did not return an account id"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        response = fetch_account(config["api_key"])
        if response.status_code != 200:
            raise Exception(f"GET /ad_account returned {response.status_code}: {response.text[:300]}")
        account = response.json()
        auth = TokenAuthenticator(token=config["api_key"])
        dated = {
            "today": account_today(account.get("timezone")),
            "currency_code": account.get("currency_code"),
            "timezone_name": account.get("timezone"),
            "authenticator": auth,
        }
        campaigns = Campaigns(authenticator=auth)
        ad_groups = AdGroups(parent=campaigns, authenticator=auth)
        return [
            AdAccount(authenticator=auth),
            campaigns,
            ad_groups,
            Ads(parent=ad_groups, authenticator=auth),
            EventSettings(authenticator=auth),
            CampaignInsights(**dated),
            *(CampaignInsights(segment=segment, **dated) for segment in ("country", "device", "platform", "product")),
            CampaignConversions(parent=campaigns, **dated),
            *(
                CampaignConversions(parent=campaigns, breakdown=breakdown, **dated)
                for breakdown in ("country", "device")
            ),
        ]
