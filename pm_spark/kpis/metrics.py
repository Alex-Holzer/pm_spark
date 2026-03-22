"""KPI metric helpers for process-mining event logs."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pm_spark._common import (
    _drop_null_case_keys,
    _duration_unit_factor,
    _timestamp_to_seconds,
    _validate_columns,
)

# Public functions keep a uniform signature for composability, so some
# parameters may be intentionally unused in specific implementations.

VALID_FREQUENCY_GRAINS = frozenset({"day", "week", "month"})


def _per_case_duration(
    df: DataFrame,
    case_col: str,
    timestamp_col: str,
    factor: float,
    value_col: str = "throughput_time_value",
) -> DataFrame:
    """One row per case: first-to-last timestamp span in the given time unit.

    Uses `_timestamp_to_seconds` for the timestamp column, then
    ``(max - min) / factor`` as DoubleType in `value_col`.
    """

    ts_seconds = _timestamp_to_seconds(df, timestamp_col)
    return df.groupBy(F.col(case_col)).agg(
        ((F.max(ts_seconds) - F.min(ts_seconds)) / F.lit(factor))
        .cast(T.DoubleType())
        .alias(value_col)
    )


def throughput_time(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    unit: str = "hours",
    output_col: str = "throughput_time",
) -> DataFrame:
    """Computes first-to-last case duration as one KPI row per case.

    The function aggregates to one row per `case_col`, calculates duration
    between earliest and latest event timestamps, and returns the duration
    in the selected `unit`, rounded to 2 decimals.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Uniform API parameter; not used by this function.
        timestamp_col: Column name for the event timestamp.
        unit: Duration unit: seconds, minutes, hours, or days.
        output_col: Name of the output duration column.

    Returns:
        DataFrame with one row per case and `output_col` as DoubleType.

    Notes:
        Rows with null case keys are excluded.
        Cases with one event return 0.0 duration.
        Cases where all timestamps are null return null duration.
    """

    _validate_columns(df, case_col, timestamp_col)
    df_valid = _drop_null_case_keys(df, case_col)
    factor = _duration_unit_factor(unit)
    per_case = _per_case_duration(
        df_valid,
        case_col,
        timestamp_col,
        factor,
        value_col="__throughput_raw",  # temp name, aliased to output_col below
    )
    return per_case.select(
        F.col(case_col),
        F.round(F.col("__throughput_raw"), 2).alias(output_col),
    )


def stp_rate(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,  # noqa: ARG001
    actor_col: str,
    technical_users: list[str],
    null_strategy: str = "exclude",
    output_col: str = "is_stp",
) -> DataFrame:
    """Flags cases where all actor values are technical users.

    A case is STP when all values in `actor_col` belong to `technical_users`.
    Null handling is controlled by `null_strategy`:
    - "exclude": null actors are ignored and cases with only null actors are
      not STP.
    - "treat_as_technical": null actors do not disqualify the case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Uniform API parameter; not used by this function.
        timestamp_col: Uniform API parameter; not used by this function.
        actor_col: Column name containing user/actor identifiers.
        technical_users: List of actor identifiers treated as technical users.
        null_strategy: Null handling mode ("exclude" or "treat_as_technical").
        output_col: Name of the output STP flag column.

    Returns:
        DataFrame with one row per case and boolean `output_col`.
    """

    _validate_columns(df, case_col, actor_col)
    df_valid = _drop_null_case_keys(df, case_col)
    if not technical_users:
        raise ValueError("`technical_users` must contain at least one value.")
    technical_user_set = list(set(technical_users))
    if null_strategy not in {"exclude", "treat_as_technical"}:
        raise ValueError(
            "`null_strategy` must be either 'exclude' or 'treat_as_technical'."
        )

    has_non_technical = (
        F.count(
            F.when(
                F.col(actor_col).isNotNull()
                & ~F.col(actor_col).isin(technical_user_set),
                1,
            )
        )
        > 0
    )
    has_present_actor = F.count(F.col(actor_col)) > 0
    is_stp_expr = (
        ~has_non_technical
        if null_strategy == "treat_as_technical"
        else (~has_non_technical & has_present_actor)
    )

    return df_valid.groupBy(F.col(case_col)).agg(is_stp_expr.alias(output_col))


def rework_rate(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,  # noqa: ARG001
    output_col: str = "has_rework",
) -> DataFrame:
    """Flags cases containing at least one repeated activity.

    A case has rework when the same non-null `activity_col` appears more than
    once within that case. Output is one row per case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Uniform API parameter; not used by this function.
        output_col: Name of the output rework flag column.

    Returns:
        DataFrame with one row per case and boolean `output_col`.
    """

    _validate_columns(df, case_col, activity_col)
    df_valid = _drop_null_case_keys(df, case_col)

    # collect_set omits nulls; count(non-null) matches — rework iff
    # non-null events > distinct non-null activities.
    return df_valid.groupBy(F.col(case_col)).agg(
        (
            F.count(F.when(F.col(activity_col).isNotNull(), F.lit(1)))
            > F.size(F.collect_set(F.col(activity_col)))
        )
        .cast(T.BooleanType())
        .alias(output_col)
    )


def activity_frequency_distribution(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,  # noqa: ARG001
    count_col: str = "activity_count",
    frequency_col: str = "activity_relative_frequency",
) -> DataFrame:
    """Computes activity counts and relative frequencies across all events.

    This KPI is intentionally not one-row-per-case, because it describes
    global activity distribution at activity-label level.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (null keys dropped).
        activity_col: Column name for the activity label.
        timestamp_col: Uniform API parameter; not used by this function.
        count_col: Name of the output activity count column.
        frequency_col: Name of the output relative-frequency column.

    Returns:
        DataFrame with one row per activity containing count and relative
        frequency as DoubleType.

    Note:
        This metric counts events, not distinct cases containing an activity.
    """

    _validate_columns(df, case_col, activity_col)
    df_valid = _drop_null_case_keys(df, case_col)
    activity_counts = df_valid.groupBy(F.col(activity_col)).agg(
        F.count(F.lit(1)).alias(count_col)
    )
    total_rows = activity_counts.agg(F.sum(F.col(count_col)).alias("_total")).collect()
    raw_total = total_rows[0]["_total"] if total_rows else None
    total_count = 0 if raw_total is None else raw_total
    # Avoid division by zero in the plan when total_count is 0 (defensive: e.g.
    # null sum); empty activity_counts still yields no output rows.

    return activity_counts.select(
        F.col(activity_col),
        F.col(count_col).cast(T.LongType()).alias(count_col),
        F.when(
            F.lit(total_count) > 0,
            F.round(
                (F.col(count_col) / F.lit(float(total_count))).cast(T.DoubleType()),
                4,
            ),
        )
        .otherwise(F.lit(None).cast(T.DoubleType()))
        .alias(frequency_col),
    )


def case_volume_over_time(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    frequency: str = "day",
    output_col: str = "case_volume",
    sort_result: bool = True,
) -> DataFrame:
    """Counts case starts grouped by day, week, or month.

    The function first determines each case start timestamp, then aggregates
    case counts by truncated start period.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Uniform API parameter; not used by this function.
        timestamp_col: Column name for the event timestamp.
        frequency: Time grain for aggregation: day, week, or month.
        output_col: Name of the case-count output column.
        sort_result: When True, order rows by `period_start` (extra shuffle).

    Returns:
        DataFrame with one row per time bucket containing `output_col`.

    Note:
        For `frequency="week"`, week boundaries follow ISO-8601 (Monday start).
    """

    _validate_columns(df, case_col, timestamp_col)
    df_valid = _drop_null_case_keys(df, case_col)
    normalized = frequency.lower()
    if normalized not in VALID_FREQUENCY_GRAINS:
        raise ValueError("Invalid frequency. Expected one of: day, week, month.")

    case_starts = df_valid.groupBy(F.col(case_col)).agg(
        F.min(F.col(timestamp_col)).alias("case_start_ts")
    )

    grouped = (
        case_starts.withColumn(
            "period_start", F.date_trunc(normalized, F.col("case_start_ts"))
        )
        .groupBy(F.col("period_start"))
        .agg(F.count(F.col(case_col)).cast(T.LongType()).alias(output_col))
    )
    return grouped.orderBy(F.col("period_start")) if sort_result else grouped


def throughput_time_percentiles(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    unit: str = "hours",
    accuracy: int = 10_000,
    p50_col: str = "p50",
    p75_col: str = "p75",
    p90_col: str = "p90",
    p95_col: str = "p95",
) -> DataFrame:
    """Computes throughput-time percentile KPIs from per-case durations.

    Durations are based on first-to-last timestamp per case and converted into
    the selected `unit`. The result contains one row with P50, P75, P90, P95,
    each rounded to 2 decimals.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Uniform API parameter; not used by this function.
        timestamp_col: Column name for the event timestamp.
        unit: Duration unit: seconds, minutes, hours, or days.
        accuracy: Positive integer controlling percentile approximation
            precision. Higher values increase accuracy at higher memory cost.
            Passed directly to PySpark `percentile_approx` as accuracy.
        p50_col: Name of the P50 output column.
        p75_col: Name of the P75 output column.
        p90_col: Name of the P90 output column.
        p95_col: Name of the P95 output column.

    Returns:
        Single-row DataFrame with percentile KPI columns.

    Notes:
        Rows with null case keys are excluded.
        Returns null percentile values when no valid cases exist.
    """

    _validate_columns(df, case_col, timestamp_col)
    df_valid = _drop_null_case_keys(df, case_col)
    if accuracy < 1:
        raise ValueError("`accuracy` must be a positive integer.")

    factor = _duration_unit_factor(unit)
    per_case_duration = _per_case_duration(
        df_valid, case_col, timestamp_col, factor, "throughput_time_value"
    )
    pcts = F.percentile_approx(
        F.col("throughput_time_value"),
        F.array(F.lit(0.50), F.lit(0.75), F.lit(0.90), F.lit(0.95)),
        accuracy,
    )

    return per_case_duration.agg(
        F.round(
            pcts[0].cast(T.DoubleType()),
            2,
        ).alias(p50_col),
        F.round(
            pcts[1].cast(T.DoubleType()),
            2,
        ).alias(p75_col),
        F.round(
            pcts[2].cast(T.DoubleType()),
            2,
        ).alias(p90_col),
        F.round(
            pcts[3].cast(T.DoubleType()),
            2,
        ).alias(p95_col),
    )
