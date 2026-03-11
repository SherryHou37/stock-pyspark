# stock-pyspark-project

This project fetches 2025 daily closing prices for the top 100 S&P 500 companies from Polygon.io and answers four PySpark analytics questions.

Before running the project, register for a free Polygon.io (Massive) account and create an API key.

## Project structure

```text
stock-pyspark-project/
├── README.md
├── requirements.txt
├── main.py
├── data/
│   └── sp500_top100.csv
├── src/
│   ├── api_client.py
│   ├── spark_session.py
│   ├── transforms.py
│   └── analytics.py
├── output/
└── tests/
    └── test_analytics.py
```

## Setup

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Export your Polygon API key:

```bash
export POLYGON_API_KEY="your_api_key_here"
```

## Run

Use the API directly:

```bash
python main.py --input-csv data/sp500_top100.csv --output-dir output
```

Run with Spark explicitly:

```bash
"$SPARK_HOME/bin/spark-submit" --master local[*] main.py --input-csv data/sp500_top100.csv --output-dir output/full_run --request-interval-seconds 12.5
```

Reuse an existing downloaded price file:

```bash
python main.py --input-csv data/sp500_top100.csv --prices-input output/daily_prices.csv --output-dir output
```

## Output

- `output/daily_prices.csv`: fetched daily close prices.
- `output/results.csv`: one row per analytics result.

The `results.csv` dataset contains:

- `max_relative_gain`: stock with the largest full-period relative gain.
- `equal_weight_portfolio_end_value`: end-of-year value for the simulated USD 1,000,000 portfolio.
- `max_monthly_cagr_jan_to_jun`: best monthly CAGR between January and June.
- `max_single_week_drop`: largest week-over-week loss and the relevant week.

## Final answers

The following answers come from `output/full_run/results.csv` generated from the full `data/sp500_top100.csv` run.

- a. Maximum relative gain: `GE` (`General Electric Company`) had the highest full-year relative gain at `0.827095`, rising from `168.59` on `2025-01-02` to `308.03` on `2025-12-31`.
- b. Simulated investment: allocating USD `10,000` to each company and reinvesting leftover cash using the `maximize_utilization` whole-share strategy produced an end-of-year portfolio value of USD `1,122,149.00`.
- b. Capital usage detail: the strategy invested USD `999,981.37` in whole shares, left USD `18.63` idle, and used pooled residual cash to buy additional shares across lower-priced names so that almost all capital was deployed.
- c. Highest monthly CAGR from January to June: `CVS` (`CVS Health Corporation`) at `0.076922`, based on a move from `44.22` on `2025-01-02` to `68.98` on `2025-06-30`.
- d. Largest single-week drop: `NFLX` (`Netflix Inc.`) with a weekly return of `-0.906210` during the week starting `2025-11-17` and ending `2025-11-21`, falling from `1112.17` to `104.31`.

## Design notes

- The API client sleeps between requests to respect the free-tier rate limit.
- Daily prices are requested with `adjusted=false`, which keeps dividends out of the calculation.
- Portfolio simulation buys only whole shares.
- Leftover cash handling is configurable:
  - `maximize_utilization` buys additional shares from cheaper names first to minimize idle cash.
  - `expensive_first` follows a high-price-first allocation if you need to mirror an interview discussion.
- Known ticker renames are normalized in the API client (`FB -> META`, `ANTM -> ELV`).

## Test

```bash
pytest
```

The tests are skipped automatically when `pyspark` is not installed.
