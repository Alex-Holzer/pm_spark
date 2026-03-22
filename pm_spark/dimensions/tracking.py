"""Dimension tracking and case-level dimension analytics for event logs."""

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


def _prepare_case_chronology(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    event_order_col: Optional[str],
) -> Tuple[DataFrame, Window, str, bool]:
    """Per-case chronological window with timestamp, activity, and tie-break."""

    _validate_columns(df, case_col, activity_col, timestamp_col)
    _validate_timestamp_column(df, timestamp_col)
    df_ord, ord_col, drop_ord = _resolve_event_order_column(df, event_order_col)
    w_ord = Window.partitionBy(case_col).orderBy(
        timestamp_col,
        activity_col,
        ord_col,
    )
    return df_ord, w_ord, ord_col, drop_ord


def distinct_nonnull_count_per_case(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "distinct_nonnull_count",
) -> DataFrame:
    """Adds per-case count of distinct non-null dimension values.

    Spark ``countDistinct`` ignores nulls. A result of 1 means the dimension
    took a single value across the case; N means N distinct states.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (signature alignment).
        timestamp_col: Column name for the event timestamp (validated).
        dimension_col: Dimension column to analyse.
        output_col: Name of the output column (LongType).

    Returns:
        Event-level DataFrame with `output_col` on every row (via groupBy +
        broadcast join; Spark does not support ``countDistinct`` in window
        frames).
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    _validate_timestamp_column(df, timestamp_col)
    per_case = df.filter(F.col(case_col).isNotNull()).groupBy(F.col(case_col)).agg(
        F.countDistinct(F.col(dimension_col)).cast(T.LongType()).alias(output_col)
    )
    return _case_metrics_broadcast_join(df, case_col, per_case)


def most_frequent_value_per_case(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "most_frequent_value",
    tiebreak_order: str = "lex",
) -> DataFrame:
    """Adds the modal non-null dimension value per case.

    Null dimension rows are excluded before frequency is computed. Ties on
    count are broken by ascending order on ``dimension_col``. When
    ``tiebreak_order`` is ``\"lex\"`` (default), ordering uses
    ``dimension_col`` cast to string — **numeric columns use lexicographic
    string order** (e.g. ``\"10\"`` before ``\"9\"``), not numeric order. Use
    ``tiebreak_order=\"numeric\"`` only when ``dimension_col`` is already a
    numeric type and numeric tie-break is intended.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (signature alignment).
        timestamp_col: Column name for the event timestamp (validated).
        dimension_col: Dimension column to analyse.
        output_col: Name of the representative value column.
        tiebreak_order: ``\"lex\"`` for string-cast order, ``\"numeric\"`` for
            ``dimension_col.asc()`` on the native type.

    Returns:
        Event-level DataFrame with `output_col` broadcast from the winning
        (case_key, value) pair per case.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    _validate_timestamp_column(df, timestamp_col)
    if tiebreak_order not in ("lex", "numeric"):
        raise ValueError('tiebreak_order must be "lex" or "numeric".')
    df_nn = df.filter(
        F.col(case_col).isNotNull() & F.col(dimension_col).isNotNull()
    )
    counts = df_nn.groupBy(F.col(case_col), F.col(dimension_col)).agg(
        F.count(F.lit(1)).alias("_freq")
    )
    tiebreak_expr = (
        F.col(dimension_col).asc()
        if tiebreak_order == "numeric"
        else F.col(dimension_col).cast("string").asc()
    )
    w_mode = Window.partitionBy(case_col).orderBy(
        F.col("_freq").desc(),
        tiebreak_expr,
    )
    winners = (
        counts.withColumn("_rn", F.row_number().over(w_mode))
        .filter(F.col("_rn") == 1)
        .select(F.col(case_col), F.col(dimension_col).alias(output_col))
    )
    return _case_metrics_broadcast_join(df, case_col, winners)


def nonnull_entry_count_per_case(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "nonnull_entry_count",
) -> DataFrame:
    """Adds per-case count of non-null dimension rows (density, not cardinality).

    PySpark ``count(column)`` in a window counts non-null values only.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp (unused here).
        dimension_col: Dimension column to analyse.
        output_col: Name of the output count column (LongType).

    Returns:
        Event-level DataFrame with `output_col` on every row.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    w_case = Window.partitionBy(case_col)
    return df.withColumn(
        output_col,
        F.count(F.col(dimension_col)).over(w_case).cast(T.LongType()),
    )


def dimension_stability_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "is_dimension_stable",
) -> DataFrame:
    """True when the case has exactly one distinct non-null dimension value."""

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    per_case = df.filter(F.col(case_col).isNotNull()).groupBy(F.col(case_col)).agg(
        F.countDistinct(F.col(dimension_col)).cast(T.LongType()).alias("_distinct_n")
    )
    joined = _case_metrics_broadcast_join(df, case_col, per_case)
    return joined.withColumn(output_col, F.col("_distinct_n") == F.lit(1)).drop(
        "_distinct_n"
    )


def number_of_value_changes(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "dimension_value_changes",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Counts dimension value changes on non-null rows only (ordered by time).

    Null dimension rows are dropped before ``lag``; e.g. A → null → B yields
    one change (A to B). The count is attached to every event row for the case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        dimension_col: Dimension column to analyse.
        output_col: Name of the per-case change count column (LongType).
        event_order_col: Optional column for deterministic ordering when
            timestamp and activity tie.

    Returns:
        Event-level DataFrame with `output_col`.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    df_nn = df.filter(
        F.col(case_col).isNotNull() & F.col(dimension_col).isNotNull()
    ).cache()
    try:
        # _ord_col / _drop_ord unused here: per_case groupBy(case_col).agg(...)
        # projects only the sum; synthetic __pm_event_ord never reaches the join.
        df_ord, w_ord, _ord_col, _drop_ord = _prepare_case_chronology(
            df_nn,
            case_col,
            activity_col,
            timestamp_col,
            event_order_col,
        )
        with_lag = df_ord.withColumn("_prev", F.lag(F.col(dimension_col)).over(w_ord))
        step = with_lag.withColumn(
            "_step",
            F.when(
                F.col("_prev").isNotNull() & (F.col(dimension_col) != F.col("_prev")),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        per_case = step.groupBy(F.col(case_col)).agg(
            F.sum(F.col("_step")).cast(T.LongType()).alias(output_col)
        )
        return (
            _case_metrics_broadcast_join(df, case_col, per_case)
            .withColumn(
                output_col,
                F.coalesce(F.col(output_col), F.lit(0).cast(T.LongType())),
            )
        )
    finally:
        df_nn.unpersist()


def first_nonnull_value(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "first_nonnull_value",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Earliest non-null dimension value per case (by event time, tie-broken).

    Uses the same struct ``groupBy`` + broadcast join pattern as
    ``eventlog.preparation.first_nonnull_in_dimension`` for consistent
    ordering when combined with that module.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (signature alignment).
        timestamp_col: Column name for the event timestamp.
        dimension_col: Dimension column to evaluate.
        output_col: Name of the first non-null value column to add.
        event_order_col: Optional column for deterministic ordering within ties.

    Returns:
        Event-level DataFrame with ``output_col`` on every row.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
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


def last_nonnull_value(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    output_col: str = "last_nonnull_value",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Latest non-null dimension value per case (by event time, tie-broken).

    Matches ``eventlog.preparation.last_nonnull_in_dimension`` semantics.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (signature alignment).
        timestamp_col: Column name for the event timestamp.
        dimension_col: Dimension column to evaluate.
        output_col: Name of the last non-null value column to add.
        event_order_col: Optional column for deterministic ordering within ties.

    Returns:
        Event-level DataFrame with ``output_col`` on every row.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
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


def value_at_activity(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    target_activity: str,
    output_col: str = "dimension_value_at_activity",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Dimension value at the first occurrence of ``target_activity`` per case.

    If the activity never occurs for a case, ``output_col`` is null on all rows
    for that case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        dimension_col: Dimension column to read at the hit event.
        target_activity: Activity label to locate (first hit by time wins).
        output_col: Output column name joined back to the full log.
        event_order_col: Optional column for deterministic ordering when
            timestamp and activity tie.

    Returns:
        Original event log with `output_col` filled per case from the hit row.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    df_ord, w_hit, _, _ = _prepare_case_chronology(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
    )
    hits = df_ord.filter(F.col(activity_col) == F.lit(target_activity))
    first_hit = (
        hits.withColumn("_rn", F.row_number().over(w_hit))
        .filter(F.col("_rn") == 1)
        .select(F.col(case_col), F.col(dimension_col).alias(output_col))
    )
    # Join original df (not df_ord): tie-break column lives only on df_ord for
    # row_number(); first_hit has no synthetic cols and output must match df.
    return _case_metrics_broadcast_join(df, case_col, first_hit)


def multi_value_case_rate(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
) -> float:
    """Returns the share of cases with more than one distinct dimension value.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp (unused here).
        dimension_col: Dimension column to analyse.

    Returns:
        Rate in ``[0.0, 1.0]`` (Python float; uses one driver ``collect``).
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    per_case = df.filter(F.col(case_col).isNotNull()).groupBy(F.col(case_col)).agg(
        F.countDistinct(F.col(dimension_col)).alias("_d")
    )
    agg_row = per_case.agg(
        F.avg((F.col("_d") > 1).cast(T.DoubleType())).alias("_rate"),
    ).collect()[0]
    rate = agg_row["_rate"]
    return float(rate) if rate is not None else 0.0


def null_coverage_rate(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    per_case_null_share_col: str = "null_share_per_case",
    global_null_share_col: str = "global_null_share",
) -> DataFrame:
    """Adds per-case and global null shares for a dimension on the event log.

    Per-case: ``(event_count - nonnull_count) / event_count``. Global: same
    ratio over all events. Global share is collected once on the driver and
    attached as a literal column.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label (unused here).
        timestamp_col: Column name for the event timestamp (validated).
        dimension_col: Dimension column to analyse.
        per_case_null_share_col: Output column for per-case null share.
        global_null_share_col: Output column for log-wide null share.

    Returns:
        Event-level DataFrame with both share columns (DoubleType). The
        internal cached plan is unpersisted in ``finally``; the returned
        DataFrame may remain cached until the caller unpersists it.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    _validate_timestamp_column(df, timestamp_col)
    w_case = Window.partitionBy(case_col)
    ev_ct = F.count(F.lit(1)).over(w_case).cast(T.DoubleType())
    nn_ct = F.count(F.col(dimension_col)).over(w_case).cast(T.DoubleType())
    enriched = (
        df.withColumn("_ev_ct", ev_ct)
        .withColumn("_nn_ct", nn_ct)
        .withColumn(
            per_case_null_share_col,
            F.when(
                F.col("_ev_ct") > 0,
                (F.col("_ev_ct") - F.col("_nn_ct")) / F.col("_ev_ct"),
            )
            .otherwise(F.lit(None))
            .cast(T.DoubleType()),
        )
        .drop("_ev_ct", "_nn_ct")
    )
    enriched.cache()
    try:
        global_scalar_row = enriched.agg(
            F.count(F.lit(1)).cast(T.DoubleType()).alias("_te"),
            F.sum(
                F.when(F.col(dimension_col).isNull(), F.lit(1)).otherwise(F.lit(0))
            ).cast(T.DoubleType()).alias("_nz"),
        ).collect()[0]
        te = global_scalar_row["_te"]
        nz = global_scalar_row["_nz"]
        global_val = None
        if te is not None and te > 0 and nz is not None:
            global_val = float(nz / te)
        result = enriched.withColumn(
            global_null_share_col,
            F.lit(global_val).cast(T.DoubleType()),
        )
        result.cache()
        result.count()
        return result
    finally:
        enriched.unpersist()


def dimension_value_transition_matrix(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    dimension_col: str,
    from_col: str = "from_value",
    to_col: str = "to_value",
    transition_count_col: str = "transition_count",
    drop_self_transitions: bool = False,
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Counts consecutive dimension transitions per case (nulls retained).

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        dimension_col: Dimension column for transitions.
        from_col: Name of the lagged (previous) value column in the output.
        to_col: Name of the current value column in the output.
        transition_count_col: Name of the count column (LongType).
        drop_self_transitions: If True, drops rows where ``from_col`` equals
            ``to_col`` (including null-to-null).
        event_order_col: Optional column for deterministic ordering when
            timestamp and activity tie.

    Returns:
        Long-format DataFrame with ``from_col``, ``to_col``, and
        ``transition_count_col``. Row order is undefined; use
        ``.orderBy(transition_count_col.desc(), ...)`` for display.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col, dimension_col)
    df_ord, w_ord, _, _ = _prepare_case_chronology(
        df,
        case_col,
        activity_col,
        timestamp_col,
        event_order_col,
    )
    edges = (
        df_ord.filter(F.col(case_col).isNotNull())
        .withColumn(from_col, F.lag(F.col(dimension_col)).over(w_ord))
        .withColumn(to_col, F.col(dimension_col))
    )
    pairs = edges
    if drop_self_transitions:
        pairs = pairs.filter(~F.col(from_col).eqNullSafe(F.col(to_col)))

    return pairs.groupBy(F.col(from_col), F.col(to_col)).agg(
        F.count(F.lit(1)).cast(T.LongType()).alias(transition_count_col)
    )
