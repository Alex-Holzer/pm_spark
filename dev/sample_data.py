
from datetime import datetime
from typing import List, Dict, Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Edge cases covered by this sample log (used for manual verification):
# - Duplicate activity in a case (activity "B" appears twice in C004)
# - Missing expected activity (activity "B" is absent in C003)
# - Parallel events with the exact same timestamp (activities "B" and "P" in C005)
# - Activity loop in the case trace (A -> B -> A in C006)
# - Dimension null patterns:
#   - Leading null dimension values (resource is null at the case start in C003, C006)
#   - Middle null dimension values requiring carry-forward (resource is null mid-case in C004)
#   - Dimension column with all-null values in a case (resource is null for all events in C007)
# - Null department entries to exercise null coverage rates (dept is null in several cases)
#
# Column conventions:
# - case_key: case identifier
# - activity: activity label
# - timestamp: event timestamp
# - resource, department: dimension columns for dimension-tracking functions
#
def create_sample_event_log(spark: SparkSession) -> DataFrame:
    """Create a small Spark DataFrame covering key process-mining edge cases.

    Returns an event log with columns `case_key`, `activity`, `timestamp`,
    `resource`, and `department`. The dataset is intentionally small (<60 rows)
    but contains duplicates, missing activities, parallel timestamps, loops,
    and null patterns in dimension columns to support manual verification.

    Args:
        spark: Active SparkSession.

    Returns:
        DataFrame containing event-level rows for multiple case traces.
    """

    schema = T.StructType(
        [
            T.StructField("case_key", T.StringType(), False),
            T.StructField("activity", T.StringType(), False),
            T.StructField("timestamp", T.TimestampType(), False),
            T.StructField("resource", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
        ]
    )

    rows: List[Dict[str, Any]] = [
        # Day 1: 2026-03-18
        {
            "case_key": "C001",
            "activity": "A",
            "timestamp": datetime(2026, 3, 18, 9, 0, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C001",
            "activity": "B",
            "timestamp": datetime(2026, 3, 18, 9, 5, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C001",
            "activity": "C",
            "timestamp": datetime(2026, 3, 18, 9, 10, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C001",
            "activity": "D",
            "timestamp": datetime(2026, 3, 18, 9, 20, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C002",
            "activity": "A",
            "timestamp": datetime(2026, 3, 18, 9, 0, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C002",
            "activity": "B",
            "timestamp": datetime(2026, 3, 18, 9, 4, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C002",
            "activity": "Manual_Review",
            "timestamp": datetime(2026, 3, 18, 9, 8, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C002",
            "activity": "C",
            "timestamp": datetime(2026, 3, 18, 9, 15, 0),
            "resource": "R2",
            "department": "D2",
        },
        {
            "case_key": "C002",
            "activity": "D",
            "timestamp": datetime(2026, 3, 18, 9, 25, 0),
            "resource": "R2",
            "department": "D2",
        },
        # Missing activity "B"
        {
            "case_key": "C003",
            "activity": "A",
            "timestamp": datetime(2026, 3, 18, 10, 0, 0),
            "resource": None,
            "department": "D1",
        },
        {
            "case_key": "C003",
            "activity": "C",
            "timestamp": datetime(2026, 3, 18, 10, 10, 0),
            "resource": "R3",
            "department": None,
        },
        {
            "case_key": "C003",
            "activity": "D",
            "timestamp": datetime(2026, 3, 18, 10, 30, 0),
            "resource": "R3",
            "department": "D3",
        },
        # Duplicate activity "B"
        {
            "case_key": "C004",
            "activity": "A",
            "timestamp": datetime(2026, 3, 18, 11, 0, 0),
            "resource": "R1",
            "department": "D1",
        },
        {
            "case_key": "C004",
            "activity": "B",
            "timestamp": datetime(2026, 3, 18, 11, 5, 0),
            "resource": "R1",
            "department": "D1",
        },
        # resource is intentionally null to test carry-forward behavior
        {
            "case_key": "C004",
            "activity": "B",
            "timestamp": datetime(2026, 3, 18, 11, 8, 0),
            "resource": None,
            "department": "D1",
        },
        {
            "case_key": "C004",
            "activity": "C",
            "timestamp": datetime(2026, 3, 18, 11, 18, 0),
            "resource": "R4",
            "department": None,
        },
        {
            "case_key": "C004",
            "activity": "D",
            "timestamp": datetime(2026, 3, 18, 11, 30, 0),
            "resource": "R4",
            "department": "D2",
        },
        # Day 2: 2026-03-19
        # Parallel events at the same timestamp ("B" and "P")
        {
            "case_key": "C005",
            "activity": "A",
            "timestamp": datetime(2026, 3, 19, 9, 0, 0),
            "resource": "R2",
            "department": "D1",
        },
        {
            "case_key": "C005",
            "activity": "B",
            "timestamp": datetime(2026, 3, 19, 9, 5, 0),
            "resource": "R2",
            "department": None,
        },
        {
            "case_key": "C005",
            "activity": "P",
            "timestamp": datetime(2026, 3, 19, 9, 5, 0),
            "resource": "R3",
            "department": "D4",
        },
        {
            "case_key": "C005",
            "activity": "C",
            "timestamp": datetime(2026, 3, 19, 9, 15, 0),
            "resource": "R3",
            "department": "D4",
        },
        {
            "case_key": "C005",
            "activity": "D",
            "timestamp": datetime(2026, 3, 19, 9, 20, 0),
            "resource": "R3",
            "department": "D4",
        },
        # Loop: A -> B -> A
        {
            "case_key": "C006",
            "activity": "A",
            "timestamp": datetime(2026, 3, 19, 9, 0, 0),
            "resource": None,
            "department": "D1",
        },
        {
            "case_key": "C006",
            "activity": "B",
            "timestamp": datetime(2026, 3, 19, 9, 6, 0),
            "resource": "R5",
            "department": None,
        },
        {
            "case_key": "C006",
            "activity": "A",
            "timestamp": datetime(2026, 3, 19, 9, 12, 0),
            "resource": "R5",
            "department": "D2",
        },
        {
            "case_key": "C006",
            "activity": "C",
            "timestamp": datetime(2026, 3, 19, 9, 18, 0),
            "resource": "R5",
            "department": "D2",
        },
        {
            "case_key": "C006",
            "activity": "D",
            "timestamp": datetime(2026, 3, 19, 9, 30, 0),
            "resource": "R6",
            "department": "D2",
        },
        # All-null dimension in a case (resource is null for every event)
        {
            "case_key": "C007",
            "activity": "A",
            "timestamp": datetime(2026, 3, 19, 12, 0, 0),
            "resource": None,
            "department": None,
        },
        {
            "case_key": "C007",
            "activity": "B",
            "timestamp": datetime(2026, 3, 19, 12, 5, 0),
            "resource": None,
            "department": "D5",
        },
        {
            "case_key": "C007",
            "activity": "C",
            "timestamp": datetime(2026, 3, 19, 12, 15, 0),
            "resource": None,
            "department": None,
        },
        {
            "case_key": "C007",
            "activity": "D",
            "timestamp": datetime(2026, 3, 19, 12, 25, 0),
            "resource": None,
            "department": "D5",
        },
    ]

    # NOTE: For manual verification only: this function is for a tiny dataset.
    # The Spark DataFrame is created with an explicit schema to keep types stable.
    df = spark.createDataFrame(rows, schema=schema)

    # Ensure Spark doesn't treat timestamps as strings.
    df = df.withColumn("timestamp", F.col("timestamp").cast(T.TimestampType()))

    return df

