import csv
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def load_symbols_dataframe(spark, input_csv_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_csv_path)
        .withColumnRenamed("symbol", "requested_symbol")
    )


def load_price_dataframe(spark, input_csv_path: str) -> DataFrame:
    price_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_csv_path)
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("close_price", F.col("close_price").cast("double"))
    )
    return price_df


def write_results(dataframe: DataFrame, output_csv_path: str) -> None:
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            writer.writerow([row[column] for column in dataframe.columns])
