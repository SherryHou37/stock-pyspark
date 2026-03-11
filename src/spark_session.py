from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "stock-pyspark-project") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
