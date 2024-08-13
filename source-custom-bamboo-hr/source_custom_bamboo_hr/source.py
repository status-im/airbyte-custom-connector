#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import logging
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpSubStream, HttpStream
from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator

logger = logging.getLogger("airbyte")

FIELDS_PARAMS = "id,firstName,lastName,displayedName,division,team,department,customENSUsername,customStatusPublicKey,customGitHubusername,customDiscordUsername,supervision,hireDate"

class CustomBambooHrStream(HttpStream, ABC):
    url_base = "https://api.bamboohr.com/api/gateway.php/"

    def __init__(self, organisation: str, **kwargs):
        super().__init__(**kwargs)
        self.organisation = organisation

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
       return None


    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return { "Accept" : "application/json"}



class Employees(CustomBambooHrStream):
    primary_key= "id"


    def path(self, **kwargs) -> str:
        return f"{self.organisation}/v1/employees/directory"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.info("Response: %s - %s", response.status_code, response.json())
        data = response.json()
        for e in data.get("employees"):
            yield e

class EmployeesDetails(HttpSubStream, Employees):
    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, 
        stream_slice: Mapping[str, Any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        employee_id = stream_slice.get("parent").get("id")
        return f"{self.organisation}/v1/employees/{employee_id}?fields={FIELDS_PARAMS}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        logger.debug("Response: %s",response.json())
        data = response.json()
        yield data




# Source
class SourceCustomBambooHr(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BasicHttpAuthenticator(username=config["token"], password="x")
        employees = Employees(organisation=config["organisation"], authenticator=auth)
        return [ 
                employees, 
                EmployeesDetails(organisation=config["organisation"], authenticator=auth, parent=employees)
            ]
