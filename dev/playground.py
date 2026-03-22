
from pyspark.sql import SparkSession

from dev.sample_data import create_sample_event_log


def _display_or_show(df):
    # Databricks provides `display()`. Local Spark uses `show()`.
    try:
        display(df)
    except NameError:
        df.show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pm_spark_playground")
        .getOrCreate()
    )

    df = create_sample_event_log(spark)
    _display_or_show(df)
    spark.stop()
