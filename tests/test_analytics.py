import importlib.util

import pytest


pyspark_available = importlib.util.find_spec("pyspark") is not None
pytestmark = pytest.mark.skipif(
    not pyspark_available, reason="pyspark is not installed"
)


def test_pyspark_dependency_present():
    if not pyspark_available:
        pytest.skip("pyspark is not installed")
    assert pyspark_available


if pyspark_available:
    from pyspark.sql import functions as F

    from src.analytics import (
        combine_results,
        compute_max_relative_gain,
        compute_max_weekly_drop,
        compute_monthly_cagr,
        simulate_equal_weight_portfolio,
    )
    from src.spark_session import create_spark_session

    @pytest.fixture(scope="module")
    def spark():
        session = create_spark_session("stock-pyspark-tests")
        yield session
        session.stop()

    @pytest.fixture(scope="module")
    def symbols_df(spark):
        return spark.createDataFrame(
            [
                ("AAA Corp", "AAA"),
                ("BBB Corp", "BBB"),
            ],
            ["company_name", "requested_symbol"],
        )

    @pytest.fixture(scope="module")
    def prices_df(spark):
        return spark.createDataFrame(
            [
                ("AAA", "AAA", "2025-01-02", 10.0),
                ("AAA", "AAA", "2025-01-31", 12.0),
                ("AAA", "AAA", "2025-02-28", 15.0),
                ("AAA", "AAA", "2025-06-30", 18.0),
                ("AAA", "AAA", "2025-07-04", 16.0),
                ("AAA", "AAA", "2025-07-11", 12.0),
                ("AAA", "AAA", "2025-12-31", 25.0),
                ("BBB", "BBB", "2025-01-02", 100.0),
                ("BBB", "BBB", "2025-01-31", 98.0),
                ("BBB", "BBB", "2025-02-28", 96.0),
                ("BBB", "BBB", "2025-06-30", 95.0),
                ("BBB", "BBB", "2025-07-04", 90.0),
                ("BBB", "BBB", "2025-07-11", 70.0),
                ("BBB", "BBB", "2025-12-31", 90.0),
            ],
            ["symbol", "requested_symbol", "trade_date", "close_price"],
        ).withColumn(
            "trade_date",
            F.to_date("trade_date"),
        )

    def test_max_relative_gain(prices_df, symbols_df):
        row = compute_max_relative_gain(prices_df, symbols_df).collect()[0]
        assert row.symbol == "AAA"
        assert row.value == pytest.approx(1.5)

    def test_portfolio_simulation(prices_df, symbols_df):
        row = simulate_equal_weight_portfolio(prices_df, symbols_df, 10000.0).collect()[
            0
        ]
        assert row.metric == "equal_weight_portfolio_end_value"
        assert row.value > 0
        assert "cash_remaining=" in row.details

    def test_monthly_cagr(prices_df, symbols_df):
        row = compute_monthly_cagr(prices_df, symbols_df).collect()[0]
        assert row.symbol == "AAA"
        assert row.value > 0

    def test_max_weekly_drop(prices_df, symbols_df):
        row = compute_max_weekly_drop(prices_df, symbols_df).collect()[0]
        assert row.symbol == "AAA"
        assert row.value < 0

    def test_combine_results(prices_df, symbols_df):
        combined = combine_results(
            compute_max_relative_gain(prices_df, symbols_df),
            simulate_equal_weight_portfolio(prices_df, symbols_df, 10000.0),
            compute_monthly_cagr(prices_df, symbols_df),
            compute_max_weekly_drop(prices_df, symbols_df),
        )
        assert combined.count() == 4
