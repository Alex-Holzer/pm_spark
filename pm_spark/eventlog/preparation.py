"""Event-log preparation helpers with uniform public function signatures.

All public functions follow the project signature standard:
`(df, case_col, activity_col, timestamp_col, ...) -> DataFrame`.
Some functions keep `activity_col` in the signature for consistency even
when not used by the internal logic.

Chronological helpers accept an optional ``chronological_window`` built with
``make_chronological_window`` so pipelines can reuse one ``Window`` across
``DataFrame.transform`` steps. Use ``detect_concurrency`` to flag same-timestamp
events within a case and attach a sorted activity array for each instant.
"""

from typing import Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pm_spark._common import (
    _case_metrics_broadcast_join,
    _drop_order_column_if_needed,
    _nonnull_dimension_metrics,
    _resolve_event_order_column,
    _validate_columns,
    _validate_timestamp_column,
)

_LAST_COUNT_COL = "__pm_last_n"


def make_chronological_window(
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    *tiebreak_cols: str,
) -> Window:
    """Builds a shared per-case chronological ``Window`` for injection into helpers.

    Use this once in a pipeline, then pass the result as ``chronological_window``
    to preparation functions so partition/order keys stay aligned. Include any
    source-system event id columns in ``tiebreak_cols`` for stable ordering.

    Args:
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        *tiebreak_cols: Extra column names appended to the ``orderBy`` list.

    Returns:
        ``Window`` partitioned by ``case_col`` and ordered by ``timestamp_col``,
        ``activity_col``, then each tie-break column in order.

    Note:
        Unlike the internally generated chronological window, this spec has no
        row-level tie-breaker beyond ``tiebreak_cols``. Pass a stable unique
        event-id column in ``tiebreak_cols`` to match deterministic ordering
        when timestamps and activities tie.
    """

    order_cols = (timestamp_col, activity_col) + tiebreak_cols
    return Window.partitionBy(case_col).orderBy(*order_cols)


def _prepare_chronological_window(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    event_order_col: Optional[str],
    chronological_window: Optional[Window] = None,
) -> Tuple[DataFrame, Window, Optional[str], bool]:
    """Validates inputs and returns a per-case chronological window spec."""

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    if chronological_window is not None:
        return df, chronological_window, None, False
    df_ord, ord_col, drop_ord = _resolve_event_order_column(df, event_order_col)
    w_case = Window.partitionBy(case_col).orderBy(
        timestamp_col,
        activity_col,
        ord_col,
    )
    return df_ord, w_case, ord_col, drop_ord


def detect_concurrency(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    concurrency_flag_col: str = "is_concurrent",
    concurrent_block_col: str = "concurrent_activities",
) -> DataFrame:
    """Flags events that share the same case timestamp and builds stable blocks.

    Rows with more than one event at the same ``timestamp_col`` inside a case
    are marked concurrent. For those rows, all activity labels at that
    instant are collected into an array and sorted lexicographically so
    parallel groups have one canonical representation (e.g. A and B always
    become ``[A, B]`` when string-ordered). Non-concurrent rows get a
    single-element array.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        concurrency_flag_col: Boolean column marking shared timestamps.
        concurrent_block_col: Array column of string activities at that instant.

    Returns:
        DataFrame with ``concurrency_flag_col`` and ``concurrent_block_col``.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    act_str = F.col(activity_col).cast("string")
    instant_agg = df.groupBy(case_col, timestamp_col).agg(
        F.count(F.lit(1)).alias("__pm_cnt"),
        F.array_sort(F.collect_list(act_str)).alias("__pm_block"),
    )
    return (
        df.join(F.broadcast(instant_agg), on=[case_col, timestamp_col], how="left")
        .withColumn(concurrency_flag_col, F.col("__pm_cnt") > 1)
        .withColumn(
            concurrent_block_col,
            F.when(F.col(concurrency_flag_col), F.col("__pm_block")).otherwise(
                F.array(act_str)
            ),
        )
        .drop("__pm_cnt", "__pm_block")
    )


def activity_sequence_number(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "activity_index",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds a per-case sequential activity index ordered by event timestamp.

    The function returns the input DataFrame with a new column `output_col`
    containing `row_number()` within each `case_col`, ordered by ascending
    `timestamp_col`, then `activity_col`, then a final tie-breaker. By default
    the tie-breaker is `monotonically_increasing_id()` (materialised as a
    hidden column) so ordering is stable even when many rows share the same
    timestamp and activity. Pass `event_order_col` to use your own stable
    per-row key instead.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the output column added to the DataFrame.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window from
            ``make_chronological_window``; when set, ``event_order_col`` is
            ignored.

    Returns:
        DataFrame with one additional column `output_col` containing the
        sequential activity index (starting at 1) per case.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    out = df_ord.withColumn(output_col, F.row_number().over(w_case).cast("long"))
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def activity_occurrence_index(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "activity_occurrence_index",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Adds a per-activity occurrence counter within each case.

    Unlike ``activity_sequence_number`` (which ranks all events globally within a
    case), this counts how many times this activity label has occurred up to
    and including the current row in chronological order. The first occurrence
    of any activity is ``1``; the second occurrence of the same activity is
    ``2``. Values greater than ``1`` flag repeat executions (rework) at event
    level.

    This helper does not accept ``chronological_window``: Spark ``Window``
    objects are opaque, so a case-level injected window cannot be replayed on
    ``partitionBy(case_col, activity_col)``. Pipelines that use
    ``make_chronological_window(..., tiebreak_col)`` elsewhere should pass the
    same tie-break column as ``event_order_col`` here so ordering matches.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the output column added to the DataFrame.
        event_order_col: Optional column for deterministic ordering within ties
            (same role as for ``activity_sequence_number``). If omitted, a
            synthetic ``monotonically_increasing_id`` column is used.

    Returns:
        DataFrame with one additional long column ``output_col`` (1-based
        occurrence index per ``(case_col, activity_col)``).

    Example:
        >>> df_out = activity_occurrence_index(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="activity_occurrence_index",
        ... )
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    df_ord, ord_col, drop_ord = _resolve_event_order_column(df, event_order_col)
    w_activity = Window.partitionBy(case_col, activity_col).orderBy(
        timestamp_col,
        ord_col,
    )
    out = df_ord.withColumn(
        output_col,
        F.row_number().over(w_activity).cast("long"),
    )
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def activity_position_ratio(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "activity_position_ratio",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds the normalized chronological position of each event within its case.

    Computed as ``(activity_sequence_number - 1) / (event_count - 1)``, yielding
    ``0.0`` for the first event and ``1.0`` for the last. Single-event cases
    receive ``0.0``. The ratio is comparable across cases of different lengths.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the output column added to the DataFrame.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional double column ``output_col`` in ``[0, 1]``.

    Example:
        >>> df_out = activity_position_ratio(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="activity_position_ratio",
        ... )
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    w_case_total = w_case.rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing,
    )
    seq = F.row_number().over(w_case).cast("double")
    total = F.count(F.lit(1)).over(w_case_total).cast("double")
    ratio = F.when(total <= 1.0, F.lit(0.0)).otherwise((seq - 1.0) / (total - 1.0))
    out = df_ord.withColumn(output_col, ratio)
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def case_start_timestamp(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    output_col: str = "case_start_ts",
) -> DataFrame:
    """Adds the per-case first event timestamp as a new column.

    The function adds `output_col` to every event row, where each case's
    start timestamp is the minimum `timestamp_col` value. Case-level minima
    are computed with `groupBy` and merged via a broadcast join.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the start timestamp column to add.

    Returns:
        DataFrame with one additional timestamp column `output_col`.
    """

    _validate_columns(df, case_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    metrics = df.groupBy(case_col).agg(
        F.min(F.col(timestamp_col)).alias(output_col),
    )
    return _case_metrics_broadcast_join(df, case_col, metrics)


def case_end_timestamp(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    output_col: str = "case_end_ts",
) -> DataFrame:
    """Adds the per-case last event timestamp as a new column.

    The function adds `output_col` to every event row, where each case's
    end timestamp is the maximum `timestamp_col` value. Case-level maxima
    are computed with `groupBy` and merged via a broadcast join.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the end timestamp column to add.

    Returns:
        DataFrame with one additional timestamp column `output_col`.
    """

    _validate_columns(df, case_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    metrics = df.groupBy(case_col).agg(
        F.max(F.col(timestamp_col)).alias(output_col),
    )
    return _case_metrics_broadcast_join(df, case_col, metrics)


def case_throughput_time(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    output_col: str = "case_throughput_time_us",
) -> DataFrame:
    """Adds total case duration in microseconds to every event row.

    Computed as ``unix_micros(max(timestamp)) - unix_micros(min(timestamp))``
    per case. Single-event cases yield ``0``. Uses ``groupBy`` and a broadcast
    join, consistent with ``case_start_timestamp`` and ``case_end_timestamp``.
    Microseconds match ``time_since_previous_event`` for unit-consistent ratios.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the duration column to add (microseconds).

    Returns:
        DataFrame with one additional long column ``output_col``.

    Example:
        >>> df_out = case_throughput_time(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="case_throughput_time_us",
        ... )
    """

    _validate_columns(df, case_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    ts = F.col(timestamp_col)
    metrics = df.groupBy(case_col).agg(
        (F.unix_micros(F.max(ts)) - F.unix_micros(F.min(ts))).alias(output_col),
    )
    return _case_metrics_broadcast_join(df, case_col, metrics)


def time_since_case_start(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    output_col: str = "time_since_case_start_us",
) -> DataFrame:
    """Adds elapsed microseconds from the case start to each event.

    The chronologically first event in a case receives ``0``. Later events
    receive the delta from the case minimum timestamp. Units match
    ``time_since_previous_event`` and ``case_throughput_time``.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the elapsed-time column to add (microseconds).

    Returns:
        DataFrame with one additional column ``output_col``.

    Example:
        >>> df_out = time_since_case_start(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="time_since_case_start_us",
        ... )
    """

    _validate_columns(df, case_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    # Window (not groupBy+broadcast): combines per-row unix_micros with case min
    # in one plan; a join would reshuffle without benefit.
    w_case = Window.partitionBy(case_col)
    case_start_us = F.min(F.unix_micros(F.col(timestamp_col))).over(w_case)
    return df.withColumn(
        output_col,
        F.unix_micros(F.col(timestamp_col)) - case_start_us,
    )


def remaining_case_time(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    output_col: str = "remaining_case_time_us",
) -> DataFrame:
    """Adds remaining case time in microseconds from each event to case end.

    The chronologically last event in a case receives ``0``. Earlier events
    receive the delta to the case maximum timestamp. Units match
    ``time_since_previous_event`` and ``case_throughput_time``.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the remaining-time column to add (microseconds).

    Returns:
        DataFrame with one additional column ``output_col``.

    Example:
        >>> df_out = remaining_case_time(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="remaining_case_time_us",
        ... )
    """

    _validate_columns(df, case_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    # Window (not groupBy+broadcast): combines per-row unix_micros with case max
    # in one plan; a join would reshuffle without benefit.
    w_case = Window.partitionBy(case_col)
    case_end_us = F.max(F.unix_micros(F.col(timestamp_col))).over(w_case)
    return df.withColumn(
        output_col,
        case_end_us - F.unix_micros(F.col(timestamp_col)),
    )


def is_first_activity_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "is_first_activity",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds a boolean flag for the first event row within each case.

    Exactly one row per case is flagged `True`, matching the row where
    `activity_sequence_number` equals 1 under the same ordering rules.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the boolean first-event flag column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional boolean column `output_col`.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    seq = F.row_number().over(w_case)
    out = df_ord.withColumn(output_col, seq == 1)
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def is_last_activity_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "is_last_activity",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds a boolean flag for the last event row within each case.

    Exactly one row per case is flagged `True`, matching the row with the
    largest sequence index from the same ordering as `activity_sequence_number`.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        output_col: Name of the boolean last-event flag column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional boolean column `output_col`.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    counts = df_ord.groupBy(case_col).agg(
        F.count(F.lit(1)).cast("long").alias(_LAST_COUNT_COL)
    )
    df_cnt = _case_metrics_broadcast_join(df_ord, case_col, counts)
    out = (
        df_cnt.withColumn(
            output_col,
            F.row_number().over(w_case) == F.col(_LAST_COUNT_COL),
        ).drop(_LAST_COUNT_COL)
    )
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def first_occurrence_of_activity(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    activity_name: str,
    output_col: str = "first_activity_ts",
) -> DataFrame:
    """Adds first occurrence timestamp for a named activity per case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        activity_name: Activity label to search for within each case.
        output_col: Name of the first occurrence timestamp column to add.

    Returns:
        DataFrame with one additional timestamp column `output_col`.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    w_case = Window.partitionBy(case_col)
    ts_when_activity = F.when(
        F.col(activity_col) == activity_name, F.col(timestamp_col)
    )
    return df.withColumn(output_col, F.min(ts_when_activity).over(w_case))


def last_occurrence_of_activity(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    activity_name: str,
    output_col: str = "last_activity_ts",
) -> DataFrame:
    """Adds last occurrence timestamp for a named activity per case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        activity_name: Activity label to search for within each case.
        output_col: Name of the last occurrence timestamp column to add.

    Returns:
        DataFrame with one additional timestamp column `output_col`.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    w_case = Window.partitionBy(case_col)
    ts_when_activity = F.when(
        F.col(activity_col) == activity_name, F.col(timestamp_col)
    )
    return df.withColumn(output_col, F.max(ts_when_activity).over(w_case))


def _activity_match_count(
    activity_col: str,
    activity_name: str,
    window_spec: Window,
) -> F.Column:
    """Builds a windowed aggregate counting matches of `activity_name`."""

    return F.count(
        F.when(F.col(activity_col) == activity_name, 1)
    ).over(window_spec)


def duplicate_activity_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,  # noqa: ARG001
    activity_name: str,
    output_col: str = "has_duplicate_activity",
) -> DataFrame:
    """Flags cases where a named activity occurs more than once.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp (unused here).
        activity_name: Activity label to evaluate for duplication per case.
        output_col: Name of the boolean duplicate flag column to add.

    Returns:
        DataFrame with one additional boolean column `output_col`.
    """

    _validate_columns(df, case_col, activity_col)
    w_case = Window.partitionBy(case_col)
    match_count = _activity_match_count(activity_col, activity_name, w_case)

    return df.withColumn(output_col, match_count > 1)


def missing_activity_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,  # noqa: ARG001
    expected_activity: str,
    output_col: str = "is_missing_activity",
) -> DataFrame:
    """Flags cases where a required activity never occurs.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp (unused here).
        expected_activity: Activity label expected to appear in every case.
        output_col: Name of the boolean missing-activity flag column to add.

    Returns:
        DataFrame with one additional boolean column `output_col`.
    """

    _validate_columns(df, case_col, activity_col)
    w_case = Window.partitionBy(case_col)
    match_count = _activity_match_count(activity_col, expected_activity, w_case)

    return df.withColumn(output_col, match_count == 0)


def event_count_per_case(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,  # noqa: ARG001
    output_col: str = "event_count_per_case",
) -> DataFrame:
    """Adds total event count per case to each row.

    Counts are computed with `groupBy` and merged via a broadcast join.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp (unused here).
        output_col: Name of the per-case event count column to add.

    Returns:
        DataFrame with one additional long column `output_col`.
    """

    _validate_columns(df, case_col)
    metrics = df.groupBy(case_col).agg(
        F.count(F.lit(1)).alias(output_col),
    )
    joined = _case_metrics_broadcast_join(df, case_col, metrics)
    return joined.withColumn(output_col, F.col(output_col).cast("long"))


def carry_forward_dimension_value(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "dimension_value_filled",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Carries forward the latest non-null dimension value within each case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Activity column used as the second chronological sort key
            after ``timestamp_col`` within each case (via
            ``_prepare_chronological_window``).
        timestamp_col: Column name for the event timestamp ordering.
        dimension_col: Dimension column to forward-fill.
        output_col: Name of the forward-filled output column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional column `output_col`.
    """

    df_ord, w_base, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    _validate_columns(df_ord, dimension_col)
    w_case_order = w_base.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    out = df_ord.withColumn(
        output_col, F.last(F.col(dimension_col), ignorenulls=True).over(w_case_order)
    )
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def first_nonnull_in_dimension(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "first_nonnull_value",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Adds the first non-null dimension value per case to every row.

    The value is the dimension on the chronologically earliest row (with
    tie-breaking) where the dimension is non-null. Implemented with
    `groupBy` and a broadcast join to avoid unbounded window frames.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp ordering.
        dimension_col: Dimension column to evaluate.
        output_col: Name of the first non-null value column to add.
        event_order_col: Optional column for deterministic ordering within ties.

    Returns:
        DataFrame with one additional column `output_col`.
    """

    _validate_columns(df, case_col, timestamp_col, dimension_col)
    _validate_timestamp_column(df, timestamp_col)
    df_ord, ord_col, drop_ord = _resolve_event_order_column(df, event_order_col)
    metrics = _nonnull_dimension_metrics(
        df_ord,
        case_col,
        timestamp_col,
        dimension_col,
        ord_col,
        use_max=False,
    ).select(
        F.col(case_col),
        F.col("_picked._val").alias(output_col),
    )
    out = _case_metrics_broadcast_join(df_ord, case_col, metrics)
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def last_nonnull_in_dimension(
    df: DataFrame,
    case_col: str,
    activity_col: str,  # noqa: ARG001
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "last_nonnull_value",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Adds the last non-null dimension value per case to every row.

    The value is the dimension on the chronologically latest row (with
    tie-breaking) where the dimension is non-null. Implemented with
    `groupBy` and a broadcast join to avoid unbounded window frames.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp ordering.
        dimension_col: Dimension column to evaluate.
        output_col: Name of the last non-null value column to add.
        event_order_col: Optional column for deterministic ordering within ties.

    Returns:
        DataFrame with one additional column `output_col`.
    """

    _validate_columns(df, case_col, timestamp_col, dimension_col)
    _validate_timestamp_column(df, timestamp_col)
    df_ord, ord_col, drop_ord = _resolve_event_order_column(df, event_order_col)
    metrics = _nonnull_dimension_metrics(
        df_ord,
        case_col,
        timestamp_col,
        dimension_col,
        ord_col,
        use_max=True,
    ).select(
        F.col(case_col),
        F.col("_picked._val").alias(output_col),
    )
    out = _case_metrics_broadcast_join(df_ord, case_col, metrics)
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def time_since_previous_event(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "microseconds_since_previous_event",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds elapsed microseconds from previous event within each case.

    Uses ``unix_micros`` on the validated timestamp column so both
    ``TimestampType`` and ``TimestampNTZType`` are handled without casting
    to time-zone-aware timestamp. The first event in a case has null in
    ``output_col``.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp ordering.
        output_col: Name of the elapsed-microseconds output column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional long column `output_col`.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    ts_col = F.col(timestamp_col)
    prev_ts = F.lag(ts_col).over(w_case)
    diff_us = F.unix_micros(ts_col) - F.unix_micros(prev_ts)
    out = df_ord.withColumn(output_col, diff_us)
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def previous_activity(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "previous_activity",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds the previous activity label within each case.

    The first event in a case receives null.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp ordering.
        output_col: Name of the previous-activity output column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional string column `output_col`.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    out = df_ord.withColumn(output_col, F.lag(F.col(activity_col)).over(w_case))
    return _drop_order_column_if_needed(out, ord_col, drop_ord)


def next_activity(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "next_activity",
    event_order_col: Optional[str] = None,
    chronological_window: Optional[Window] = None,
) -> DataFrame:
    """Adds the next activity label within each case.

    The last event in a case receives null.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp ordering.
        output_col: Name of the next-activity output column to add.
        event_order_col: Optional column for deterministic ordering within ties.
        chronological_window: Optional injected window; when set,
            ``event_order_col`` is ignored.

    Returns:
        DataFrame with one additional string column `output_col`.
    """

    df_ord, w_case, ord_col, drop_ord = _prepare_chronological_window(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
        chronological_window,
    )
    out = df_ord.withColumn(output_col, F.lead(F.col(activity_col)).over(w_case))
    return _drop_order_column_if_needed(out, ord_col, drop_ord)
