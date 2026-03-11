import argparse
import os
from pathlib import Path

from src.analytics import (
    combine_results,
    compute_max_relative_gain,
    compute_max_weekly_drop,
    compute_monthly_cagr,
    simulate_equal_weight_portfolio,
)
from src.api_client import PolygonClient
from src.spark_session import create_spark_session
from src.transforms import (
    load_price_dataframe,
    load_symbols_dataframe,
    write_results,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Polygon-backed stock analytics with PySpark."
    )
    parser.add_argument(
        "--input-csv",
        default="data/sp500_top100.csv",
        help="Path to the input company list CSV.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where result files will be written.",
    )
    parser.add_argument(
        "--prices-input",
        default=None,
        help="Optional existing daily price CSV. When set, API calls are skipped.",
    )
    parser.add_argument(
        "--start-date",
        default="2025-01-01",
        help="Inclusive start date for Polygon queries.",
    )
    parser.add_argument(
        "--end-date",
        default="2025-12-31",
        help="Inclusive end date for Polygon queries.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("POLYGON_API_KEY"),
        help="Polygon API key. Defaults to POLYGON_API_KEY from the environment.",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=12.5,
        help="Delay between API calls to stay within free-tier rate limits.",
    )
    parser.add_argument(
        "--leftover-strategy",
        choices=["maximize_utilization", "expensive_first"],
        default="maximize_utilization",
        help="How pooled leftover cash is reused for the investment simulation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        symbols_df = load_symbols_dataframe(spark, args.input_csv)
        if args.prices_input:
            # Reuse a cached price file when the API download step is not needed.
            prices_df = load_price_dataframe(spark, args.prices_input)
        else:
            if not args.api_key:
                raise ValueError(
                    "Polygon API key is required when --prices-input is not provided. "
                    "Set POLYGON_API_KEY or pass --api-key."
                )
            client = PolygonClient(
                api_key=args.api_key,
                request_interval_seconds=args.request_interval_seconds,
            )
            raw_prices_path = output_dir / "daily_prices.csv"
            # Persist raw API output first so the analysis can be rerun offline.
            client.fetch_prices_to_csv(
                symbols_csv_path=args.input_csv,
                output_csv_path=str(raw_prices_path),
                start_date=args.start_date,
                end_date=args.end_date,
            )
            prices_df = load_price_dataframe(spark, str(raw_prices_path))

        # Compute each interview question separately and merge them into one result file.
        max_gain_df = compute_max_relative_gain(prices_df, symbols_df)
        portfolio_df = simulate_equal_weight_portfolio(
            prices_df=prices_df,
            symbols_df=symbols_df,
            allocation_per_stock=10000.0,
            leftover_strategy=args.leftover_strategy,
        )
        monthly_cagr_df = compute_monthly_cagr(prices_df, symbols_df)
        max_weekly_drop_df = compute_max_weekly_drop(prices_df, symbols_df)

        results_df = combine_results(
            max_gain_df=max_gain_df,
            portfolio_df=portfolio_df,
            monthly_cagr_df=monthly_cagr_df,
            max_weekly_drop_df=max_weekly_drop_df,
        )

        write_results(results_df, str(output_dir / "results.csv"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
