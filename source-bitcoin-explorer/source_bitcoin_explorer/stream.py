from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources.streams.http import HttpStream
import requests, datetime

class Transactions(HttpStream):

    url_base = "https://blockchain.info/rawaddr/"
    primary_key = ["hash"]

    @staticmethod
    def get_trx_movement(sent_sats: int, received_sats: int) -> str:
        movement = "none"
        if sent_sats > 0 and received_sats > 0:
            movement = "both"
        elif sent_sats > 0:
            movement = "out"
        elif received_sats > 0:
            movement = "in"

        return movement

    def __init__(self, wallets: List[dict], **kwargs):
        super().__init__(**kwargs)
        self.wallets = wallets
        self.__request_limit = 100
        self.previous_day_mapping = {
            wallet["address"]: datetime.datetime.now().date() - datetime.timedelta(days=1)
            for wallet in wallets
            if not wallet.get("backfill", False)
        }

    def stream_slices(self, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        for wallet in self.wallets:
            yield {
                "address": wallet['address'],
                "name": wallet["name"],
                "tags": wallet["tags"]
            }

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return stream_slice['address']

    def parse_response(self, response: requests.Response, stream_slice: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping]:
        data: dict = response.json()
        txs: list = data.get("txs", [])
        wallet_address: str = stream_slice["address"]
        ref_date: Optional[datetime.date] = self.previous_day_mapping.get(wallet_address)

        if not ref_date:
            self.logger.info(f"Processing {len(txs)} transactions for {wallet_address}")
        else:
            self.logger.info(f"Processing wallet {wallet_address} transactions for {ref_date}")

        for trx in txs:
            sent_sats = sum(
                inp["prev_out"]["value"]
                for inp in trx["inputs"]
                if inp.get("prev_out", {}).get("addr") == wallet_address
            )

            received_sats = sum(
                out["value"]
                for out in trx["out"]
                if out.get("addr") == wallet_address
            )

            trx_timestamp = datetime.datetime.fromtimestamp(trx["time"])
            current = {
                "timestamp": trx_timestamp,
                "timezone": "UTC", # As it is in pytz
                "hash": trx["hash"],
                "chain": "bitcoin",
                "chain_id": None,
                "wallet_name": stream_slice["name"],
                "wallet_address": wallet_address,
                "tags": stream_slice["tags"],
                "total_transactions": data["n_tx"],
                "total_received": data["total_received"],
                "total_sent": data["total_sent"],
                "token_name": "Bitcoin",
                "token_symbol": "BTC",
                "token_decimal": 8,
                "transaction_fee": trx["fee"],
                "net_change": received_sats - sent_sats, # received - sent
                "movement": Transactions.get_trx_movement(sent_sats, received_sats),
                "current_balance": trx["balance"],
                "sent": sent_sats,
                "received": received_sats,
                "from_utxo": [
                    {"addr": item["prev_out"].get("addr"), "amount": item["prev_out"]["value"]}
                    for item in trx["inputs"]
                    if item.get("prev_out")
                ],
                "to_utxo": [
                    {"addr": item.get("addr"), "value": item["value"]}
                    for item in trx["out"]
                    if item.get("addr")
                ]
            }

            if ref_date and ref_date != trx_timestamp.date():
                continue

            yield current


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data: dict = response.json()
        txs = data.get("txs", [])

        if not txs:
            return None

        ref_date: Optional[datetime.date] = self.previous_day_mapping.get(data["address"])

        url = response.request.url
        sub_string = "offset="

        offset = int(url.split(sub_string)[-1]) if sub_string in url else 0
        next_offset = offset + self.__request_limit
        params = {
            "offset": next_offset
        }
        # Backfill pagination
        if not ref_date and next_offset < data["n_tx"]:
            return params

        # Non-backfill - paginate until ref_date exists
        page_dates = {
            datetime.datetime.fromtimestamp(trx["time"]).date()
            for trx in txs
        }
        if ref_date in page_dates and next_offset < data["n_tx"]:
            return params

        return None

    def request_params(self, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None, **kwargs):
        params = {
            "limit": self.__request_limit
        }
        if next_page_token:
            params["offset"] = next_page_token["offset"]

        return params

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code != 429:
            return None

        # Note: Retry-After is 24 hours
        seconds = int(response.headers.get("Retry-After", 0))
        self.logger.info(f"Sleeping for {seconds}s ({seconds // (60 * 60)}) hrs")
        return seconds
