
from typing import Any, Mapping, Union

import requests
import logging
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.requests_native_auth import (
    BasicHttpAuthenticator,
    SingleUseRefreshTokenOauth2Authenticator,
    TokenAuthenticator,
)
from airbyte_cdk.utils import AirbyteTracedException

logger = logging.getLogger("airbyte")

class TwitterOAuth(SingleUseRefreshTokenOauth2Authenticator):
    """
    https://developer.x.com/en/docs/authentication/oauth-2-0/user-access-token
    """

    def build_refresh_request_headers(self) -> Mapping[str, Any]:
        logger.info("Refreshing token")
        return {
            "Authorization": BasicHttpAuthenticator(self.get_client_id(), self.get_client_secret()).token,
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def build_refresh_request_body(self) -> Mapping[str, Any]:
        return {
            "grant_type": self.get_grant_type(),
            "refresh_token": self.get_refresh_token(),
        }

    def _get_refresh_access_token_response(self) -> Mapping[str, Any]:
        response = requests.request(
            method="POST",
            url=self.get_token_refresh_endpoint(),
            data=self.build_refresh_request_body(),
            headers=self.build_refresh_request_headers(),
        )
        content = response.json()
        logger.info("Refresh - response status code %s", response.status_code)
        if response.status_code == 400 and content.get("error") == "invalid_request":
            logger.error("Error when refreshing token: %s", content)
            raise AirbyteTracedException(
                internal_message=content.get("error_description"),
                message="Refresh token is invalid or expired. Please re-authenticate to restore access to Twitter API.",
                failure_type=FailureType.config_error,
            )
        response.raise_for_status()
        return content

