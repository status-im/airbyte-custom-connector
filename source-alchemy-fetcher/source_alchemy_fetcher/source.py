import datetime, logging, requests, requests.auth, copy, time
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union, Tuple
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")

class HistoricalRates(HttpStream):

    REQUESTS = 25
    http_method = "POST"
    primary_key = ["symbol", "timestamp"]
    interval_mappings = {
        "5m": 7,
        "1h": 30,
        "1d": 365
    }

    @staticmethod
    def to_utc_format(date: Union[str, datetime.date], midnight: bool = True) -> str:
        
        if isinstance(date, str):
            date = datetime.datetime.strptime(date, "%Y-%m-%d")
        
        format = "%Y-%m-%dT" + ("00:00:00" if midnight else "23:59:59") + "Z"
        return date.strftime(format)
    
    @staticmethod
    def to_date(date: Optional[Union[str, datetime.datetime, datetime.date]]) -> datetime.date:
        
        if isinstance(date, datetime.date):
            return date
        
        if isinstance(date, datetime.datetime):
            return date.date()

        is_blank = isinstance(date, type(None))
        is_string = isinstance(date, str)

        if is_blank or (is_string and len(date) == 0):
            return datetime.datetime.now().date() - datetime.timedelta(days=1)

        return datetime.datetime.strptime(date, "%Y-%m-%d").date()

    @staticmethod
    def create_payload(token_info: dict) -> dict:
        """
        Create REST API payload for either `PricesApiEndpointsGetHistoricalTokenPricesRequest0`
        or `PricesApiEndpointsGetHistoricalTokenPricesRequest1` from
        https://www.alchemy.com/docs/data/prices-api/prices-api-endpoints/prices-api-endpoints/get-historical-token-prices

        Parameters:
            - `token_info` - Element from Airbyte's `tokens` config

        Output:
            - the payload
        """        
        payload = {
            "startTime": HistoricalRates.to_utc_format(
                HistoricalRates.to_date(token_info.get("start_date"))
            ),
            "endTime": HistoricalRates.to_utc_format(
                HistoricalRates.to_date(token_info.get("end_date")),
                False
            ),
            "interval": token_info["interval"],
            "withMarketData": True
        }

        if "network" in token_info and "address" in token_info:
            payload.update({
                "network": token_info["network"],
                "address": token_info["address"]
            })
        
        else:
            payload.update({
                "symbol": token_info["symbol"]
            })

        return payload


    def __init__(self, tokens: List[dict], authenticator: requests.auth.AuthBase,**kwargs):
        super().__init__(authenticator, **kwargs)
        self.tokens = tokens
        self.sleep_seconds = 1 / self.REQUESTS
        

    @property
    def url_base(self) -> str:
        return f"https://api.g.alchemy.com/prices/v1/"

    @property
    def name(self) -> str:
        return "historical_rates"

    def path(self, *, stream_state = None, stream_slice = None, next_page_token = None):
        return "tokens/historical"
    
    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        
        for token_info in self.tokens:
            current_token_info = copy.deepcopy(token_info)
            current_start = token_info["start_date"]
            end_date = token_info["end_date"]
            interval = datetime.timedelta(days = self.interval_mappings[token_info["interval"]])

            while current_start <= end_date:
                current_end = current_start + interval

                current_token_info["startTime"] = current_start
                current_token_info["endTime"] = current_end

                current_slice = {
                    "payload": HistoricalRates.create_payload(current_token_info),
                    "metadata": token_info
                }
                logger.info(f"Created slice payload: {current_slice['payload']}")
                yield current_slice
                current_start = current_end + datetime.timedelta(days=1)

    def request_body_json(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> Optional[Mapping]:
        return stream_slice["payload"]
    
    def request_headers(self, **kwargs) -> MutableMapping[str, Any]:
        return {"Content-Type": "application/json"}

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        data: dict = response.json()
        symbol = stream_slice["metadata"]["symbol"]
        network = data.get("network")
        symbol_address = data.get("address")
        end_date = stream_slice["metadata"]["end_date"]
        ccy = str(data["currency"]).upper()

        for row in data["data"]:
            timestamp = datetime.datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

            if timestamp.date() > end_date:
                continue

            point = {
                "timestamp": timestamp,
                "timezone": "UTC", # As it is in pytz
                "network": network,
                "chain_id": stream_slice["metadata"]["chain_id"],
                "address": symbol_address,
                "symbol": symbol,
                "currency": ccy,
                "source": "alchemy",
                "amount": float(row["value"]),
                "market_cap": float(row["marketCap"]),
                "total_volume": float(row["totalVolume"]),
                "stream_slice": {
                    "start_date": stream_slice["payload"]["startTime"],
                    "end_date": stream_slice["payload"]["endTime"],
                    "interval": stream_slice["metadata"]["interval"]
                },
            }
            yield point

        time.sleep(self.sleep_seconds)

    def next_page_token(self, response: requests.Response) -> None:
        return None


class SourceAlchemyFetcher(AbstractSource):

    # Tested available endpoints:
    # https://www.alchemy.com/docs/choosing-a-web3-network
    chain_id_mapping = {
        "eth-mainnet": 1,
        "opt-mainnet": 10,
        "arb-mainnet": 42161,
        "base-mainnet": 8453,
        "frax-mainnet": 252,
        "zora-mainnet": 7777777
    }

    def update_tokens(self, tokens: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        new_tokens = []
        for token in tokens:
            current: dict = copy.deepcopy(token)
            
            current.update({
                "start_date": HistoricalRates.to_date(current.get("start_date")),
                "end_date": HistoricalRates.to_date(current.get("end_date")),
                "chain_id": self.chain_id_mapping.get(current.get("network"))
            })
            new_tokens.append(current)
        
        return new_tokens

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        
        url = "https://api.g.alchemy.com/prices/v1/tokens/historical"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config['api_key']}"
        }
        failed = []
        for token in config["tokens"]:

            start_date = HistoricalRates.to_date(token.get("start_date"))
            end_date = HistoricalRates.to_date(token.get("end_date"))
            if start_date > end_date:
                failed.append(f"start_date cannot be greater than end_date - {token['symbol']}")
                continue
            
            payload = HistoricalRates.create_payload(token)
            payload["interval"] = "1d"

            logger.info(f"POST request: {url}")
            logger.info(f"Payload: {payload}")
            response = requests.post(url, json=payload, headers=headers)

            output: dict = response.json()
            error: Union[str, dict[str, str]] = output.get("error")
            
            if error:
                failed.append(error["message"])
            elif response.status_code != 200:
                failed.append(f"Could not make a request for {payload['symbol']} with payload - {payload}")
            
        return len(failed) == 0, "\n".join(failed) if failed else None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = TokenAuthenticator(config["api_key"])
        return [
            HistoricalRates(self.update_tokens(config["tokens"]), auth)
        ]
        
