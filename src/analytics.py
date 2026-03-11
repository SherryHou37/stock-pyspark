import math
from typing import Dict, List

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def _attach_company_names(prices_df: DataFrame, symbols_df: DataFrame) -> DataFrame:
    return prices_df.join(symbols_df, on="requested_symbol", how="left")


def _first_last_prices(prices_df: DataFrame, symbols_df: DataFrame) -> DataFrame:
    enriched_df = _attach_company_names(prices_df, symbols_df)
    # Use window functions to capture the first and last trading day per stock.
    asc_window = Window.partitionBy("requested_symbol").orderBy(
        F.col("trade_date").asc()
    )
    desc_window = Window.partitionBy("requested_symbol").orderBy(
        F.col("trade_date").desc()
    )

    first_df = (
        enriched_df.withColumn("rank_open", F.row_number().over(asc_window))
        .filter(F.col("rank_open") == 1)
        .select(
            "requested_symbol",
            "company_name",
            F.col("trade_date").alias("start_date"),
            F.col("close_price").alias("start_close"),
        )
    )
    last_df = (
        enriched_df.withColumn("rank_close", F.row_number().over(desc_window))
        .filter(F.col("rank_close") == 1)
        .select(
            "requested_symbol",
            F.col("trade_date").alias("end_date"),
            F.col("close_price").alias("end_close"),
        )
    )
    return first_df.join(last_df, on="requested_symbol", how="inner")


def compute_max_relative_gain(prices_df: DataFrame, symbols_df: DataFrame) -> DataFrame:
    summary_df = (
        _first_last_prices(prices_df, symbols_df)
        .withColumn(
            "relative_gain",
            (F.col("end_close") - F.col("start_close")) / F.col("start_close"),
        )
        .orderBy(F.col("relative_gain").desc())
        .limit(1)
        .select(
            F.lit("max_relative_gain").alias("metric"),
            F.col("requested_symbol").alias("symbol"),
            "company_name",
            F.round("relative_gain", 6).alias("value"),
            F.col("start_date").cast("string").alias("start_date"),
            F.col("end_date").cast("string").alias("end_date"),
            F.concat_ws(
                " | ",
                F.concat(F.lit("start_close="), F.round("start_close", 4)),
                F.concat(F.lit("end_close="), F.round("end_close", 4)),
            ).alias("details"),
        )
    )
    return summary_df


def simulate_equal_weight_portfolio(
    prices_df: DataFrame,
    symbols_df: DataFrame,
    allocation_per_stock: float,
    leftover_strategy: str = "maximize_utilization",
) -> DataFrame:
    summary_rows = _first_last_prices(prices_df, symbols_df).collect()
    positions = []
    pooled_cash = 0.0

    for row in summary_rows:
        # Whole-share only purchase at the first available close of the period.
        shares = math.floor(allocation_per_stock / row.start_close)
        spent = shares * row.start_close
        leftover = allocation_per_stock - spent
        pooled_cash += leftover
        positions.append(
            {
                "symbol": row.requested_symbol,
                "company_name": row.company_name,
                "start_close": row.start_close,
                "end_close": row.end_close,
                "base_shares": shares,
                "extra_shares": 0,
            }
        )

    sortable_positions = sorted(
        positions,
        key=lambda item: item["start_close"],
        reverse=(leftover_strategy == "expensive_first"),
    )

    # Reinvest pooled leftovers one extra share at a time until no full share can be bought.
    while sortable_positions:
        purchased = False
        for position in sortable_positions:
            if pooled_cash >= position["start_close"]:
                position["extra_shares"] += 1
                pooled_cash -= position["start_close"]
                purchased = True
        if not purchased:
            break

    total_value = 0.0
    total_invested = 0.0
    total_shares = 0
    extra_share_records: List[str] = []
    for position in positions:
        owned_shares = position["base_shares"] + position["extra_shares"]
        total_shares += owned_shares
        total_invested += owned_shares * position["start_close"]
        total_value += owned_shares * position["end_close"]
        if position["extra_shares"] > 0:
            extra_share_records.append(
                f"{position['symbol']}:{position['extra_shares']}"
            )

    detail_string = " | ".join(
        [
            f"total_invested={round(total_invested, 2)}",
            f"cash_remaining={round(pooled_cash, 2)}",
            f"total_shares={total_shares}",
            f"extra_share_strategy={leftover_strategy}",
            f"extra_share_buys={';'.join(extra_share_records) if extra_share_records else 'none'}",
        ]
    )

    schema = T.StructType(
        [
            T.StructField("metric", T.StringType(), False),
            T.StructField("symbol", T.StringType(), True),
            T.StructField("company_name", T.StringType(), True),
            T.StructField("value", T.DoubleType(), False),
            T.StructField("start_date", T.StringType(), True),
            T.StructField("end_date", T.StringType(), True),
            T.StructField("details", T.StringType(), False),
        ]
    )
    row = Row(
        metric="equal_weight_portfolio_end_value",
        symbol=None,
        company_name=None,
        value=round(total_value, 6),
        start_date=None,
        end_date=None,
        details=detail_string,
    )
    return prices_df.sparkSession.createDataFrame([row], schema=schema)


def compute_monthly_cagr(prices_df: DataFrame, symbols_df: DataFrame) -> DataFrame:
    # Restrict the CAGR calculation to the January-June interview window.
    jan_jun_df = prices_df.filter(
        (F.col("trade_date") >= F.lit("2025-01-01"))
        & (F.col("trade_date") <= F.lit("2025-06-30"))
    )
    summary_df = (
        _first_last_prices(jan_jun_df, symbols_df)
        .withColumn(
            "monthly_cagr",
            F.pow(F.col("end_close") / F.col("start_close"), F.lit(1.0 / 6.0))
            - F.lit(1.0),
        )
        .orderBy(F.col("monthly_cagr").desc())
        .limit(1)
        .select(
            F.lit("max_monthly_cagr_jan_to_jun").alias("metric"),
            F.col("requested_symbol").alias("symbol"),
            "company_name",
            F.round("monthly_cagr", 6).alias("value"),
            F.col("start_date").cast("string").alias("start_date"),
            F.col("end_date").cast("string").alias("end_date"),
            F.concat_ws(
                " | ",
                F.concat(F.lit("start_close="), F.round("start_close", 4)),
                F.concat(F.lit("end_close="), F.round("end_close", 4)),
                F.lit("months=6"),
            ).alias("details"),
        )
    )
    return summary_df


def compute_max_weekly_drop(prices_df: DataFrame, symbols_df: DataFrame) -> DataFrame:
    # Collapse daily data to the final close observed in each trading week.
    week_window = Window.partitionBy("requested_symbol", "week_start").orderBy(
        F.col("trade_date").desc()
    )
    weekly_close_df = (
        prices_df.withColumn(
            "week_start", F.to_date(F.date_trunc("week", F.col("trade_date")))
        )
        .withColumn("week_rank", F.row_number().over(week_window))
        .filter(F.col("week_rank") == 1)
        .select(
            "requested_symbol",
            "week_start",
            F.col("trade_date").alias("week_end"),
            F.col("close_price").alias("week_close"),
        )
    )

    change_window = Window.partitionBy("requested_symbol").orderBy(
        F.col("week_start").asc()
    )
    weekly_returns_df = (
        weekly_close_df.withColumn(
            "previous_week_close", F.lag("week_close").over(change_window)
        )
        # Compare each week against the immediately previous week's close.
        .filter(F.col("previous_week_close").isNotNull())
        .withColumn(
            "weekly_return",
            (F.col("week_close") - F.col("previous_week_close"))
            / F.col("previous_week_close"),
        )
        .join(symbols_df, on="requested_symbol", how="left")
        .orderBy(F.col("weekly_return").asc())
        .limit(1)
        .select(
            F.lit("max_single_week_drop").alias("metric"),
            F.col("requested_symbol").alias("symbol"),
            "company_name",
            F.round("weekly_return", 6).alias("value"),
            F.col("week_start").cast("string").alias("start_date"),
            F.col("week_end").cast("string").alias("end_date"),
            F.concat_ws(
                " | ",
                F.concat(
                    F.lit("previous_week_close="), F.round("previous_week_close", 4)
                ),
                F.concat(F.lit("week_close="), F.round("week_close", 4)),
            ).alias("details"),
        )
    )
    return weekly_returns_df


def combine_results(
    max_gain_df: DataFrame,
    portfolio_df: DataFrame,
    monthly_cagr_df: DataFrame,
    max_weekly_drop_df: DataFrame,
) -> DataFrame:
    return (
        max_gain_df.unionByName(portfolio_df)
        .unionByName(monthly_cagr_df)
        .unionByName(max_weekly_drop_df)
    )
