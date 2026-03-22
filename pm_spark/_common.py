"""Shared helpers and constants for event-log DataFrames (internal use)."""

from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

SECONDS_PER_MINUTE = 60.0
SECONDS_PER_HOUR = 3600.0
SECONDS_PER_DAY = 86400.0
MILLISECONDS_PER_SECOND = 1000.0
VALID_DURATION_UNITS = frozenset({"seconds", "minutes", "hours", "days"})


def _validate_columns(df: DataFrame, *cols: str) -> None:
    """Raises an error when one or more required columns are missing."""

    missing = {col for col in cols if col not in df.columns}
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {sorted(missing)}")


def _validate_timestamp_column(df: DataFrame, timestamp_col: str) -> None:
    """Ensures ``timestamp_col`` is Spark timestamp type (with or without TZ)."""

    field = df.schema[timestamp_col]
    if not isinstance(field.dataType, (T.TimestampType, T.TimestampNTZType)):
        raise TypeError(
            "Column %r must be TimestampType or TimestampNTZType, got %s. "
            "Cast the column to timestamp before using it as an event timestamp."
            % (timestamp_col, field.dataType.simpleString())
        )


def _resolve_event_order_column(
    df: DataFrame,
    event_order_col: Optional[str],
) -> Tuple[DataFrame, str, bool]:
    """Returns ``(df_with_order, order_col_name, drop_order_col_when_done)``."""

    if event_order_col is not None:
        _validate_columns(df, event_order_col)
        return df, event_order_col, False
    tagged = df.withColumn("__pm_event_ord", F.monotonically_increasing_id())
    return tagged, "__pm_event_ord", True


def _drop_order_column_if_needed(
    df: DataFrame,
    order_col: Optional[str],
    drop: bool,
) -> DataFrame:
    if drop and order_col is not None:
        return df.drop(order_col)
    return df


def _case_metrics_broadcast_join(
    df: DataFrame,
    case_col: str,
    metrics_df: DataFrame,
    force_broadcast: bool = True,
) -> DataFrame:
    """Joins case-level metrics back to the event log (broadcast by default)."""

    join_side = F.broadcast(metrics_df) if force_broadcast else metrics_df
    return df.join(join_side, on=case_col, how="left")


def _nonnull_dimension_metrics(
    df: DataFrame,
    case_col: str,
    timestamp_col: str,
    dimension_col: str,
    order_col: str,
    use_max: bool,
) -> DataFrame:
    """Case-level first/last non-null dimension via ordered struct aggregate.

    When ``order_col`` is synthetic ``__pm_event_ord``, tie-breaking across
    tasks is not fully deterministic; pass a stable event id as
    ``event_order_col`` at the public API for production logs.
    """

    packed = F.when(
        F.col(dimension_col).isNotNull(),
        F.struct(
            F.col(timestamp_col).alias("_ts"),
            F.col(order_col).alias("_ord"),
            F.col(dimension_col).alias("_val"),
        ),
    )
    agg_fn = F.max if use_max else F.min
    return df.groupBy(case_col).agg(agg_fn(packed).alias("_picked"))


def _duration_unit_factor(unit: str) -> float:
    """Returns divisor factor to convert seconds into the requested unit."""

    normalized = unit.lower()
    factors = {
        "seconds": 1.0,
        "minutes": SECONDS_PER_MINUTE,
        "hours": SECONDS_PER_HOUR,
        "days": SECONDS_PER_DAY,
    }
    if normalized not in VALID_DURATION_UNITS:
        raise ValueError(
            "Invalid unit. Expected one of: seconds, minutes, hours, days."
        )
    return factors[normalized]


def _drop_null_case_keys(df: DataFrame, case_col: str) -> DataFrame:
    """Drops rows with null case keys to prevent phantom null-key cases."""

    return df.filter(F.col(case_col).isNotNull())


def _timestamp_to_seconds(df: DataFrame, timestamp_col: str) -> F.Column:
    """Builds a seconds expression from timestamp-like source columns.

    - TimestampType/TimestampNTZType/DateType -> cast to double epoch seconds
      (fractional seconds preserved). Casting timestamp to double uses a float;
      sub-microsecond precision can be lost in the mantissa, but duration
      metrics in hours/days are unaffected in practice.
    - LongType/IntegerType/... -> interpreted as epoch milliseconds.
    - StringType -> parsed with unix_timestamp (second precision).
    """

    data_type = df.schema[timestamp_col].dataType
    numeric_millis_types = (
        T.LongType,
        T.IntegerType,
        T.ShortType,
        T.ByteType,
    )

    if isinstance(data_type, (T.TimestampType, T.TimestampNTZType, T.DateType)):
        return F.col(timestamp_col).cast(T.DoubleType())
    if isinstance(data_type, numeric_millis_types):
        millis = F.col(timestamp_col).cast(T.DoubleType())
        return millis / F.lit(MILLISECONDS_PER_SECOND)
    if isinstance(data_type, (T.FloatType, T.DoubleType, T.DecimalType)):
        return F.col(timestamp_col).cast(T.DoubleType())
    return F.unix_timestamp(F.col(timestamp_col)).cast(T.DoubleType())
