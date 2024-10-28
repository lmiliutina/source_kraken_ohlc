from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
from airbyte_cdk.models import SyncMode, ConnectorSpecification
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


class KrakenCurrencyRateStream(HttpStream):
    def path(self, **kwargs) -> str:
        return "OHLC"
    """
    Stream to fetch OHLC data from Kraken's API.
    """
    url_base = "https://api.kraken.com/0/public/"
    primary_key = ["pair", "time"]  # Unique identifier for each record
    cursor_field = "time"  # Field used for state management

    def __init__(self, config: Mapping[str, Any]):
        super().__init__()
        self.config = config
        self.start_date = config.get('start_date')
        self.interval = config.get('interval', 1440)  # Default to 1 day
        self.quote_currencies = set(config.get('quote_currencies', ['EUR', 'USD']))
        self.base_currencies = set(config.get('base_currencies', []))
        self.asset_names = {}
        self.filtered_pairs = {}

    def get_asset_names(self):
        """
        Fetch and map Kraken asset codes to their standard names.
        """
        self.logger.debug("Fetching asset names from Kraken API")
        assets_url = f"{self.url_base}Assets"
        response = requests.get(assets_url)
        data = response.json()
        if data['error']:
            raise Exception(f"API Error: {data['error']}")
        assets = data['result']
        kraken_to_standard = {
            'XBT': 'BTC',
            'XDG': 'DOGE'
        }
        for asset_code, asset_info in assets.items():
            altname = asset_info['altname']
            standard_name = kraken_to_standard.get(altname, altname)
            self.asset_names[asset_code] = standard_name
        self.logger.debug(f"Asset names mapping: {self.asset_names}")

    def get_filtered_pairs(self):
        """
        Fetch and filter currency pairs based on base and quote currencies.
        """
        self.logger.debug("Fetching and filtering currency pairs from Kraken API")
        self.get_asset_names()
        asset_pairs_url = f"{self.url_base}AssetPairs"
        response = requests.get(asset_pairs_url)
        data = response.json()
        if data['error']:
            raise Exception(f"API Error: {data['error']}")
        asset_pairs = data['result']

        for pair, pair_info in asset_pairs.items():
            base_asset = pair_info['base']
            quote_asset = pair_info['quote']
            base_currency = self.asset_names.get(base_asset, base_asset)
            quote_currency = self.asset_names.get(quote_asset, quote_asset)

            if (quote_currency in self.quote_currencies) and (base_currency in self.base_currencies):
                self.filtered_pairs[pair] = {
                    'base_currency': base_currency,
                    'quote_currency': quote_currency
                }
        self.logger.debug(f"Filtered currency pairs: {self.filtered_pairs}")

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Define stream slices based on filtered currency pairs.
        """
        self.get_filtered_pairs()
        if not self.filtered_pairs:
            self.logger.warning("No currency pairs match the specified base and quote currencies.")
            return  # Exit the generator

        for pair, currencies in self.filtered_pairs.items():
            yield {
                "pair": pair,
                "base_currency": currencies["base_currency"],
                "quote_currency": currencies["quote_currency"],
                "since": int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp())
            }

    def request_params(
        self,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
        **kwargs
    ) -> Mapping[str, Any]:
        """
        Define query parameters for the API request.
        """
        pair = stream_slice["pair"]
        since = stream_slice.get("since", int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp()))
        params = {
            "pair": pair,
            "interval": self.interval,
            "since": since
        }
        self.logger.debug(f"Request params for pair {pair}: {params}")
        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_slice: Mapping[str, Any] = None,
        **kwargs
    ) -> Iterable[Mapping]:
        """
        Parse the API response and yield records.
        """
        data = response.json()
        if data['error']:
            raise Exception(f"API Error: {data['error']}")

        result = data['result']
        pair = stream_slice["pair"]
        records = result.get(pair, [])
        new_since = result.get('last', None)

        if not records:
            self.logger.info(f"No OHLC data returned for pair {pair}.")
            return

        for record in records:
            yield {
                "pair": pair,
                "time": datetime.utcfromtimestamp(int(record[0])).isoformat(),
                "open": float(record[1]),
                "high": float(record[2]),
                "low": float(record[3]),
                "close": float(record[4]),
                "vwap": float(record[5]),
                "volume": float(record[6]),
                "count": int(record[7]),
                "base_currency": stream_slice["base_currency"],
                "quote_currency": stream_slice["quote_currency"]
            }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        Extract the 'last' timestamp for pagination.
        """
        data = response.json()
        last = data["result"].get("last")
        if last:
            self.logger.debug(f"Next page token (since): {last}")
            return {"since": last}
        self.logger.debug("No more pages to fetch.")
        return None


    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """
        Update the stream state with the latest record's timestamp.
        """
        current_stream_state = current_stream_state or {}
        pair = latest_record["pair"]
        current_timestamp = current_stream_state.get(pair, int(datetime.strptime(self.start_date, '%Y-%m-%d').timestamp()))
        latest_timestamp = int(datetime.fromisoformat(latest_record["time"]).timestamp())
        new_timestamp = max(latest_timestamp, current_timestamp)
        current_stream_state[pair] = new_timestamp
        self.logger.debug(f"Updated state for pair {pair}: {new_timestamp}")
        return current_stream_state


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
                "count": {"type": "integer"}
            }
        }


class SourceKraken(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Optional[Any]]:
        try:
            # Perform a simple request to check connectivity
            response = requests.get("https://api.kraken.com/0/public/Time")
            if response.status_code == 200:
                logger.debug("Successfully connected to Kraken API")
                return True, None
            else:
                error_message = f"Failed to connect to Kraken API. Status code: {response.status_code}"
                logger.error(error_message)
                return False, error_message
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [KrakenCurrencyRateStream(config)]

    def spec(self, logger) -> ConnectorSpecification:
        """
        Returns the specification of the source connector, including the configuration schema.
        """
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.io/integrations/sources/kraken",
            connectionSpecification={
                "type": "object",
                "required": ["start_date", "base_currencies", "quote_currencies"],
                "properties": {
                    "start_date": {
                        "type": "string",
                        "format": "date",
                        "description": "The start date for fetching OHLC data (YYYY-MM-DD).",
                    },
                    "base_currencies": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of base currencies to fetch data for.",
                    },
                    "quote_currencies": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of quote currencies to filter pairs.",
                    },
                    "interval": {  # Optional interval parameter
                        "type": "integer",
                        "description": "Interval in minutes for OHLC data. Default is 1440 (1 day).",
                        "default": 1440
                    }
                },
            },
        )
