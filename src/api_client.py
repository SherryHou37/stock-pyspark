import csv
import time
from pathlib import Path
from typing import Dict, Iterable, List

import requests


SYMBOL_NORMALIZATION = {
    "FB": "META",
    "ANTM": "ELV",
}


class PolygonClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.polygon.io",
        request_interval_seconds: float = 12.5,
        timeout_seconds: int = 30,
        max_retries: int = 3,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.request_interval_seconds = request_interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = requests.Session()

    def fetch_prices_to_csv(
        self,
        symbols_csv_path: str,
        output_csv_path: str,
        start_date: str,
        end_date: str,
    ) -> None:
        symbols = self._read_symbols(symbols_csv_path)
        output_path = Path(output_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["symbol", "requested_symbol", "trade_date", "close_price"],
            )
            writer.writeheader()
            for index, row in enumerate(symbols, start=1):
                requested_symbol = row["symbol"]
                # Map legacy tickers from the input file to the symbol expected by Polygon.
                api_symbol = SYMBOL_NORMALIZATION.get(
                    requested_symbol, requested_symbol
                )
                records = self.fetch_daily_closes(
                    symbol=api_symbol,
                    start_date=start_date,
                    end_date=end_date,
                )
                for record in records:
                    writer.writerow(
                        {
                            "symbol": api_symbol,
                            "requested_symbol": requested_symbol,
                            "trade_date": record["trade_date"],
                            "close_price": record["close_price"],
                        }
                    )
                if index < len(symbols):
                    # Keep enough spacing between requests for the free API tier.
                    time.sleep(self.request_interval_seconds)

    def fetch_daily_closes(
        self, symbol: str, start_date: str, end_date: str
    ) -> List[Dict[str, str]]:
        endpoint = (
            f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/"
            f"{start_date}/{end_date}"
        )
        params = {
            "adjusted": "false",
            "sort": "asc",
            "limit": 5000,
            "apiKey": self.api_key,
        }

        for attempt in range(1, self.max_retries + 1):
            response = self.session.get(
                endpoint, params=params, timeout=self.timeout_seconds
            )
            if response.status_code == 429 and attempt < self.max_retries:
                # Back off progressively when Polygon signals rate limiting.
                time.sleep(self.request_interval_seconds * attempt)
                continue
            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", [])
            return [
                {
                    "trade_date": time.strftime(
                        "%Y-%m-%d", time.gmtime(item["t"] / 1000)
                    ),
                    "close_price": item["c"],
                }
                for item in results
            ]

        raise RuntimeError(f"Failed to fetch prices for {symbol} after retries.")

    @staticmethod
    def _read_symbols(symbols_csv_path: str) -> List[Dict[str, str]]:
        with open(symbols_csv_path, newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
