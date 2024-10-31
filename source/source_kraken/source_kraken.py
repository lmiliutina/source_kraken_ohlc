import json
import logging
from copy import deepcopy
from functools import lru_cache
from datetime import UTC, datetime
from typing import Any, Mapping, Iterable, Optional, MutableMapping, cast

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models.airbyte_protocol import (
    SyncMode,
    ConnectorSpecification,
)

log = logging.getLogger(__name__)


class ErrorData(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class KrakenCurrencyRateStream(HttpStream):
    """
    Stream to fetch OHLC data from Kraken's API.
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        super().__init__()
        self.config = config
        self.start_date = config['start_date']
        self.interval = config.get('interval', 1440)  # Default to 1 day
        self.quote_currencies = set(
            config.get('quote_currencies', ['EUR', 'USD'])
        )
        self.base_currencies = set(config.get('base_currencies', []))
        self.asset_names: dict[str, str] = {}
        self.filtered_pairs: dict[str, Any] = {}

    @property
    def url_base(self) -> str:
        return "https://api.kraken.com/0/public"

    @property
    def assets_url(self) -> str:
        return f"{self.url_base}/Assets"

    @property
    def asset_pairs_url(self) -> str:
        return f"{self.url_base}/AssetPairs"

    @property
    def primary_key(self) -> list[str]:
        return ["pair", "time"]

    @property
    def cursor_field(self) -> str:
        return "time"

    def path(
        self,
        *,
        stream_state: Optional[Mapping[str, Any]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> str:
        return "OHLC"

    def set_asset_names(self) -> None:
        """
        Fetch and map Kraken asset codes to their standard names.
        """
        self.logger.warning("Fetching asset names from Kraken API")
        response = requests.get(self.assets_url, timeout=10)
        data = response.json()
        if data['error']:
            raise ErrorData(f"API Error: {data['error']}")

        assets = data['result']
        kraken_to_standard = {'XBT': 'BTC', 'XDG': 'DOGE'}

        for asset_code, asset_info in assets.items():
            altname = asset_info['altname']
            standard_name = kraken_to_standard.get(altname, altname)
            self.asset_names[asset_code] = standard_name

        self.logger.debug("Asset names mapping: %s", self.asset_names)

    def set_filtered_pairs(self) -> None:
        """
        Fetch and filter currency pairs based on base and quote currencies.
        """
        self.logger.debug(
            "Fetching and filtering currency pairs from Kraken API"
        )
        self.set_asset_names()
        response = requests.get(self.asset_pairs_url, timeout=10)
        data = response.json()
        if data['error']:
            raise ErrorData(f"API Error: {data['error']}")

        asset_pairs = data['result']

        for pair, pair_info in asset_pairs.items():
            base_asset = pair_info['base']
            quote_asset = pair_info['quote']
            base_currency = self.asset_names.get(base_asset, base_asset)
            quote_currency = self.asset_names.get(quote_asset, quote_asset)

            if (quote_currency in self.quote_currencies) and (
                base_currency in self.base_currencies
            ):
                self.filtered_pairs[pair] = {
                    'base_currency': base_currency,
                    'quote_currency': quote_currency,
                }
        self.logger.debug("Filtered currency pairs: %s", self.filtered_pairs)

    def stream_slices(
        self,
        *,
        sync_mode: SyncMode,
        cursor_field: Optional[list[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define stream slices based on filtered currency pairs.
        """
        self.set_filtered_pairs()
        if not self.filtered_pairs:
            self.logger.warning(
                "No currency pairs match the specified base and quote currencies."  # noqa: E501
            )
            return

        for pair, currencies in self.filtered_pairs.items():
            yield {
                "pair": pair,
                "base_currency": currencies["base_currency"],
                "quote_currency": currencies["quote_currency"],
                "since": int(
                    datetime.strptime(self.start_date, '%Y-%m-%d').timestamp()
                ),
            }

    def request_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> MutableMapping[str, Any]:
        """
        Define query parameters for the API request.
        """
        assert stream_slice is not None
        pair = stream_slice["pair"]
        since = stream_slice.get(
            "since",
            int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp()),
        )
        params = {"pair": pair, "interval": self.interval, "since": since}
        self.logger.warning("Request params for pair %s: %s", pair, params)
        return params

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Parse the API response and yield records.
        """
        data = response.json()
        if data['error']:
            raise ErrorData(f"API Error: {data['error']}")

        result = data['result']
        assert stream_slice is not None
        pair = stream_slice["pair"]
        records = result.get(pair, None)

        if records is None or not records:
            self.logger.info("No OHLC data returned for pair %s.", pair)
            return

        for record in records:
            time = datetime.fromtimestamp(int(record[0]), UTC).isoformat()
            yield {
                "pair": pair,
                "time": time,
                "open": float(record[1]),
                "high": float(record[2]),
                "low": float(record[3]),
                "close": float(record[4]),
                "vwap": float(record[5]),
                "volume": float(record[6]),
                "count": int(record[7]),
                "base_currency": stream_slice["base_currency"],
                "quote_currency": stream_slice["quote_currency"],
            }

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        """
        Extract the 'last' timestamp for pagination.
        """
        data = response.json()
        last = data["result"].get("last")
        if last:
            self.logger.warning("Next page token (since): %s", last)
            return {"since": last}
        self.logger.warning("No more pages to fetch.")
        return None

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        """
        Update the stream state with the latest record's timestamp.
        """
        pair = latest_record["pair"]
        current_timestamp = current_stream_state.get(
            pair,
            int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp()),
        )
        latest_timestamp = int(
            datetime.fromisoformat(latest_record["time"]).timestamp()
        )
        new_timestamp = max(latest_timestamp, current_timestamp)
        current_stream_state = cast(
            MutableMapping[str, Any], deepcopy(current_stream_state)
        )
        current_stream_state[pair] = new_timestamp

        self.logger.debug("Updated state for pair %s: %s", pair, new_timestamp)
        return current_stream_state

    @lru_cache(maxsize=None)
    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "properties": {
                "pair": {"type": "string"},
                "base_currency": {"type": "string"},
                "quote_currency": {"type": "string"},
                "time": {"type": "string", "format": "date-time"},
                "open": {"type": "number"},
                "high": {"type": "number"},
                "low": {"type": "number"},
                "close": {"type": "number"},
                "vwap": {"type": "number"},
                "volume": {"type": "number"},
                "count": {"type": "integer"},
            },
        }


class SourceKraken(AbstractSource):
    # (choose from 'spec', 'check', 'discover', 'read')
    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Optional[Any]]:
        try:
            response = requests.get(
                "https://api.kraken.com/0/public/Time", timeout=10
            )
        except Exception as e:
            logger.error("Connection check failed", exc_info=e)
            return False, str(e)

        if response.status_code == 200:
            logger.debug("Successfully connected to Kraken API")
            return True, None

        error_message = (
            f"Failed to connect to Kraken API. "
            f"Status code: {response.status_code}"
        )
        logger.error(error_message)
        return False, error_message

    def streams(self, config: Mapping[str, Any]) -> list[Stream]:
        return [KrakenCurrencyRateStream(config)]

    def spec(self, logger: logging.Logger) -> ConnectorSpecification:
        """
        Returns the specification of the source connector,
        including the configuration schema.
        """
        with open("source/spec.json", "r") as file:
            data = file.read()
            spec = json.loads(data)

        return ConnectorSpecification(
            documentationUrl=spec.get("documentationUrl"),
            connectionSpecification=spec.get("connectionSpecification"),
        )
