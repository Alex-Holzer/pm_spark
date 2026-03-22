"""Local smoke check: import pm_spark and run one transform on sample data.

Run from the **repository root** (requires PySpark 3.5.x on PYTHONPATH)::

    python dev/smoke_import.py

This is not a formal test suite; it catches broken packaging or import cycles.
"""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pyspark.sql import SparkSession

from dev.sample_data import create_sample_event_log
from pm_spark import activity_sequence_number


def main() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pm_spark_smoke")
        .getOrCreate()
    )
    try:
        df = create_sample_event_log(spark)
        out = activity_sequence_number(
            df,
            case_col="case_key",
            activity_col="activity",
            timestamp_col="timestamp",
        )
        _ = out.limit(1).count()
        print("pm_spark smoke OK (import + activity_sequence_number)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
