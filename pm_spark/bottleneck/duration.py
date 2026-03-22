"""Waiting-time and bottleneck duration utilities for process-mining logs."""

from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pm_spark._common import (
    _drop_null_case_keys,
    _duration_unit_factor,
    _timestamp_to_seconds,
    _validate_columns,
)

VALID_SUMMARY_AGGS = frozenset({"avg", "sum", "max"})
VALID_ANCHORS = frozenset({"first", "last"})
VALID_ANCHOR_COMBINATIONS = frozenset(
    {
        ("first", "first"),
        ("last", "first"),
        ("first", "last"),
        ("last", "last"),
    }
)


def _case_event_order_window(
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    tiebreak_col: Optional[str] = None,
) -> Window:
    """Per-case window ordered for lag/lead on the event stream."""

    order_cols = [timestamp_col, activity_col]
    if tiebreak_col is not None:
        order_cols.append(tiebreak_col)
    return Window.partitionBy(case_col).orderBy(*order_cols)


def _anchor_agg(col_expr: F.Column, anchor: str) -> F.Column:
    """Returns min or max of `col_expr` for first vs last anchor semantics."""

    return F.min(col_expr) if anchor == "first" else F.max(col_expr)


def _with_waiting_seconds(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    tiebreak_col: Optional[str] = None,
) -> DataFrame:
    """Adds ordered predecessor columns and clamped waiting seconds."""

    ts_seconds = _timestamp_to_seconds(df, timestamp_col)
    w_case = _case_event_order_window(
        case_col, activity_col, timestamp_col, tiebreak_col
    )

    return (
        df.withColumn("event_ts_seconds", ts_seconds)
        .withColumn("prev_ts_seconds", F.lag(F.col("event_ts_seconds")).over(w_case))
        .withColumn("prev_activity", F.lag(F.col(activity_col)).over(w_case))
        .withColumn("prev_timestamp", F.lag(F.col(timestamp_col)).over(w_case))
        .withColumn(
            "waiting_seconds",
            F.greatest(F.col("event_ts_seconds") - F.col("prev_ts_seconds"), F.lit(0.0)),
        )
    )


def waiting_time_between_activities(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    unit: str = "hours",
    summary_aggs: Optional[List[str]] = None,
    tiebreak_col: Optional[str] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Computes waiting times between consecutive events per case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        unit: Duration unit: seconds, minutes, hours, or days.
        summary_aggs: Aggregations for summary output. Supported: avg, sum, max.
        tiebreak_col: Optional column name appended to the chronological
            order (after timestamp and activity) for deterministic lag when
            timestamps and activities tie.

    Returns:
        Tuple of:
        - Event-level DataFrame with waiting-time detail per transition.
        - Case-level DataFrame with requested aggregations.
    """

    cols_to_check = [case_col, activity_col, timestamp_col]
    if tiebreak_col is not None:
        cols_to_check.append(tiebreak_col)
    _validate_columns(df, *cols_to_check)
    if summary_aggs is None:
        summary_aggs = ["avg", "sum", "max"]
    if not summary_aggs:
        raise ValueError("`summary_aggs` must contain at least one aggregation.")
    invalid = set(summary_aggs) - VALID_SUMMARY_AGGS
    if invalid:
        raise ValueError(f"Invalid summary aggregations: {sorted(invalid)}")

    df_valid = _drop_null_case_keys(df, case_col)
    factor = _duration_unit_factor(unit)
    prepared = _with_waiting_seconds(
        df_valid, case_col, activity_col, timestamp_col, tiebreak_col
    )
    event_level = (
        prepared.filter(F.col("prev_timestamp").isNotNull())
        .select(
            F.col(case_col),
            F.col(activity_col),
            F.col(timestamp_col),
            F.col("prev_activity"),
            F.col("prev_timestamp"),
            F.round((F.col("waiting_seconds") / F.lit(factor)), 2)
            .cast(T.DoubleType())
            .alias("waiting_time"),
        )
    )

    waiting_for_summary = F.when(
        F.col("prev_timestamp").isNotNull(),
        (F.col("waiting_seconds") / F.lit(factor)).cast(T.DoubleType()),
    )
    agg_exprs = []
    if "avg" in summary_aggs:
        agg_exprs.append(F.round(F.avg(waiting_for_summary), 2).alias("waiting_time_avg"))
    if "sum" in summary_aggs:
        agg_exprs.append(F.round(F.sum(waiting_for_summary), 2).alias("waiting_time_sum"))
    if "max" in summary_aggs:
        agg_exprs.append(F.round(F.max(waiting_for_summary), 2).alias("waiting_time_max"))

    summary = prepared.groupBy(F.col(case_col)).agg(*agg_exprs)
    return event_level, summary


def time_between_activity_occurrences(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    activity_a: str,
    activity_b: str,
    anchor_a: str = "first",
    anchor_b: str = "first",
    unit: str = "hours",
) -> DataFrame:
    """Computes per-case duration between selected occurrences of A and B.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        activity_a: Source activity label.
        activity_b: Target activity label.
        anchor_a: Anchor for A occurrence, first or last.
        anchor_b: Anchor for B occurrence, first or last.
        unit: Duration unit: seconds, minutes, hours, or days.

    Returns:
        DataFrame with one row per case containing both activities.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    if anchor_a not in VALID_ANCHORS or anchor_b not in VALID_ANCHORS:
        raise ValueError("`anchor_a` and `anchor_b` must be 'first' or 'last'.")
    if (anchor_a, anchor_b) not in VALID_ANCHOR_COMBINATIONS:
        raise ValueError(
            "Supported anchor combinations: first-first, last-first, "
            "first-last, last-last."
        )

    df_valid = _drop_null_case_keys(df, case_col)
    factor = _duration_unit_factor(unit)
    ts_seconds = _timestamp_to_seconds(df_valid, timestamp_col)
    agg_a_sec = _anchor_agg(ts_seconds, anchor_a)
    agg_b_sec = _anchor_agg(ts_seconds, anchor_b)
    agg_a_ts = _anchor_agg(F.col(timestamp_col), anchor_a)
    agg_b_ts = _anchor_agg(F.col(timestamp_col), anchor_b)

    a_df = df_valid.filter(F.col(activity_col) == activity_a).groupBy(
        F.col(case_col)
    ).agg(
        agg_a_sec.alias("activity_a_seconds"),
        agg_a_ts.alias("activity_a_timestamp"),
    )
    b_df = df_valid.filter(F.col(activity_col) == activity_b).groupBy(
        F.col(case_col)
    ).agg(
        agg_b_sec.alias("activity_b_seconds"),
        agg_b_ts.alias("activity_b_timestamp"),
    )

    return (
        a_df.join(b_df, on=case_col, how="inner")
        .select(
            F.col(case_col),
            F.col("activity_a_timestamp"),
            F.col("activity_b_timestamp"),
            F.round(
                F.greatest(
                    (F.col("activity_b_seconds") - F.col("activity_a_seconds"))
                    / F.lit(factor),
                    F.lit(0.0),
                ),
                2,
            )
            .cast(T.DoubleType())
            .alias("duration"),
        )
    )


def activity_transition_matrix(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    transition_count_col: str = "transition_count",
    tiebreak_col: Optional[str] = None,
    sort_result: bool = True,
) -> DataFrame:
    """Counts directly-following activity pairs across all cases.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        transition_count_col: Output column for transition counts.
        tiebreak_col: Optional tie-break column for chronological ordering.
        sort_result: When True, order rows by count desc then activity names.
    """

    cols_to_check = [case_col, activity_col, timestamp_col]
    if tiebreak_col is not None:
        cols_to_check.append(tiebreak_col)
    _validate_columns(df, *cols_to_check)
    df_valid = _drop_null_case_keys(df, case_col).filter(F.col(activity_col).isNotNull())
    w_case = _case_event_order_window(
        case_col, activity_col, timestamp_col, tiebreak_col
    )

    grouped = (
        df_valid.withColumn("from_activity", F.col(activity_col))
        .withColumn("to_activity", F.lead(F.col(activity_col)).over(w_case))
        .filter(F.col("to_activity").isNotNull())
        .groupBy(F.col("from_activity"), F.col("to_activity"))
        .agg(F.count(F.lit(1)).cast(T.LongType()).alias(transition_count_col))
    )
    ordered = grouped.orderBy(
        F.col(transition_count_col).desc(),
        F.col("from_activity"),
        F.col("to_activity"),
    )
    return ordered if sort_result else grouped


def transition_time_matrix(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    unit: str = "hours",
    accuracy: int = 10_000,
    avg_col: str = "avg_transition_time",
    median_col: str = "median_transition_time",
    count_col: str = "transition_count",
    tiebreak_col: Optional[str] = None,
    sort_result: bool = True,
) -> DataFrame:
    """Computes count, average, and median transition duration for A->B pairs.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        unit: Duration unit: seconds, minutes, hours, or days.
        accuracy: Positive integer passed to percentile_approx.
        avg_col: Output column for mean transition time.
        median_col: Output column for approximate median (P50).
        count_col: Output column for transition counts.
        tiebreak_col: Optional tie-break column for chronological ordering.
        sort_result: When True, order rows by count desc then activity names.
    """

    cols_to_check = [case_col, activity_col, timestamp_col]
    if tiebreak_col is not None:
        cols_to_check.append(tiebreak_col)
    _validate_columns(df, *cols_to_check)
    if accuracy < 1:
        raise ValueError("`accuracy` must be a positive integer.")

    df_valid = _drop_null_case_keys(df, case_col).filter(F.col(activity_col).isNotNull())
    factor = _duration_unit_factor(unit)
    ts_seconds = _timestamp_to_seconds(df_valid, timestamp_col)
    w_case = _case_event_order_window(
        case_col, activity_col, timestamp_col, tiebreak_col
    )
    transition_base = df_valid.withColumn("event_ts_seconds", ts_seconds)
    next_seconds = F.lead(F.col("event_ts_seconds")).over(w_case)
    transition_seconds = F.greatest(next_seconds - F.col("event_ts_seconds"), F.lit(0.0))

    grouped = (
        transition_base.withColumn("from_activity", F.col(activity_col))
        .withColumn("to_activity", F.lead(F.col(activity_col)).over(w_case))
        .withColumn("transition_time", transition_seconds / F.lit(factor))
        .filter(F.col("to_activity").isNotNull())
        .groupBy(F.col("from_activity"), F.col("to_activity"))
        .agg(
            F.count(F.lit(1)).cast(T.LongType()).alias(count_col),
            F.round(F.avg(F.col("transition_time")), 2).cast(T.DoubleType()).alias(avg_col),
            F.round(
                F.percentile_approx(
                    F.col("transition_time"),
                    0.50,
                    accuracy,
                ).cast(T.DoubleType()),
                2,
            ).alias(median_col),
        )
    )
    ordered = grouped.orderBy(
        F.col(count_col).desc(),
        F.col("from_activity"),
        F.col("to_activity"),
    )
    return ordered if sort_result else grouped


def longest_waiting_step_per_case(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    unit: str = "hours",
    output_col: str = "longest_waiting_time",
    tiebreak_col: Optional[str] = None,
) -> DataFrame:
    """Returns per-case maximum consecutive waiting time.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        unit: Duration unit: seconds, minutes, hours, or days.
        output_col: Name of the output column for the maximum wait.
        tiebreak_col: Optional tie-break column for chronological ordering.
    """

    cols_to_check = [case_col, activity_col, timestamp_col]
    if tiebreak_col is not None:
        cols_to_check.append(tiebreak_col)
    _validate_columns(df, *cols_to_check)
    df_valid = _drop_null_case_keys(df, case_col)
    factor = _duration_unit_factor(unit)
    prepared = _with_waiting_seconds(
        df_valid, case_col, activity_col, timestamp_col, tiebreak_col
    )
    return prepared.groupBy(F.col(case_col)).agg(
        F.round(
            F.max(
                F.when(
                    F.col("prev_timestamp").isNotNull(),
                    F.col("waiting_seconds") / F.lit(factor),
                )
            ),
            2,
        )
        .cast(T.DoubleType())
        .alias(output_col)
    )


def time_to_first_occurrence_from_case_start(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    target_activity: str,
    unit: str = "hours",
    output_col: str = "time_to_first_occurrence",
) -> DataFrame:
    """Computes time from case start to first occurrence of target activity.

    Uses one ``groupBy(case_col)`` with conditional ``min`` for the target
    timestamp so case start and first target share a single shuffle.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    df_valid = _drop_null_case_keys(df, case_col)
    factor = _duration_unit_factor(unit)
    ts_seconds = _timestamp_to_seconds(df_valid, timestamp_col)

    return (
        df_valid.groupBy(F.col(case_col))
        .agg(
            F.min(ts_seconds).alias("case_start_seconds"),
            F.min(
                F.when(F.col(activity_col) == target_activity, ts_seconds)
            ).alias("target_seconds"),
        )
        .filter(F.col("target_seconds").isNotNull())
        .select(
            F.col(case_col),
            F.round(
                F.greatest(
                    (F.col("target_seconds") - F.col("case_start_seconds"))
                    / F.lit(factor),
                    F.lit(0.0),
                ),
                2,
            )
            .cast(T.DoubleType())
            .alias(output_col),
        )
    )


def parallel_activity_flag(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    output_col: str = "is_parallel",
) -> DataFrame:
    """Flags cases with more than one event at the same instant (intra-case).

    A case is flagged when at least one timestamp value appears on more than
    one row within that case (e.g. concurrent activities logged at identical
    times). Rows with null `timestamp_col` are ignored for the window; cases
    that have no non-null timestamp rows are omitted from the output.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    df_valid = _drop_null_case_keys(df, case_col)
    df_ts = df_valid.filter(F.col(timestamp_col).isNotNull())
    w_same_instant = Window.partitionBy(case_col, timestamp_col)
    return (
        df_ts.withColumn("__cnt", F.count(F.lit(1)).over(w_same_instant))
        .groupBy(F.col(case_col))
        .agg((F.max(F.col("__cnt")) > 1).cast(T.BooleanType()).alias(output_col))
    )


def activity_loop_detection(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    loop_count_col: str = "loop_count",
    loop_pairs_col: str = "loop_pairs",
    tiebreak_col: Optional[str] = None,
) -> DataFrame:
    """Detects self-loops, direct A->B->A loops, and indirect repeats per case.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        loop_count_col: Output column for loop count per case.
        loop_pairs_col: Output column for sorted loop pattern strings.
        tiebreak_col: Optional tie-break column for chronological ordering.

    Note:
        Indirect repeats use a gap ``seq - prev_same_seq > 1``; rows that close
        a direct A→X→A triple (second A) are excluded so they are not double
        counted with ``direct`` patterns. A row that closes both a direct
        triple and a longer indirect repeat is counted only under ``direct``,
        not both.
    """

    cols_to_check = [case_col, activity_col, timestamp_col]
    if tiebreak_col is not None:
        cols_to_check.append(tiebreak_col)
    _validate_columns(df, *cols_to_check)
    df_valid = _drop_null_case_keys(df, case_col).filter(F.col(activity_col).isNotNull())
    w_case = _case_event_order_window(
        case_col, activity_col, timestamp_col, tiebreak_col
    )
    w_case_activity = Window.partitionBy(case_col, activity_col).orderBy(timestamp_col)

    sequenced = (
        df_valid.withColumn("activity_next_1", F.lead(F.col(activity_col), 1).over(w_case))
        .withColumn("activity_next_2", F.lead(F.col(activity_col), 2).over(w_case))
        .withColumn("seq", F.row_number().over(w_case))
        .withColumn("prev_same_seq", F.lag(F.col("seq")).over(w_case_activity))
        .withColumn("lag1_act", F.lag(F.col(activity_col), 1).over(w_case))
        .withColumn("lag2_act", F.lag(F.col(activity_col), 2).over(w_case))
    )
    direct_triple_close = (
        F.col("lag1_act").isNotNull()
        & F.col("lag2_act").isNotNull()
        & (F.col("lag2_act") == F.col(activity_col))
        & (F.col("lag1_act") != F.col(activity_col))
    )

    self_loops = (
        sequenced.filter(F.col("activity_next_1") == F.col(activity_col))
        .select(
            F.col(case_col),
            F.concat_ws("→", F.col(activity_col), F.col(activity_col)).alias(
                "loop_pattern"
            ),
        )
        .withColumn("loop_type", F.lit("self"))
    )
    direct_loops = (
        sequenced.filter(
            F.col("activity_next_1").isNotNull()
            & F.col("activity_next_2").isNotNull()
            & (F.col(activity_col) == F.col("activity_next_2"))
        )
        .select(
            F.col(case_col),
            F.concat_ws(
                "→",
                F.col(activity_col),
                F.col("activity_next_1"),
                F.col("activity_next_2"),
            ).alias("loop_pattern"),
        )
        .withColumn("loop_type", F.lit("direct"))
    )
    indirect_loops = (
        sequenced.filter(
            F.col("prev_same_seq").isNotNull()
            & ((F.col("seq") - F.col("prev_same_seq")) > 1)
            & ~direct_triple_close
        )
        .select(
            F.col(case_col),
            F.concat(F.col(activity_col), F.lit("→...→"), F.col(activity_col)).alias(
                "loop_pattern"
            ),
        )
        .withColumn("loop_type", F.lit("indirect"))
    )

    all_loops = self_loops.unionByName(direct_loops).unionByName(indirect_loops)
    return (
        all_loops.groupBy(F.col(case_col))
        .agg(
            F.count(F.lit(1)).cast(T.LongType()).alias(loop_count_col),
            F.array_join(F.sort_array(F.collect_set(F.col("loop_pattern"))), ", ").alias(
                loop_pairs_col
            ),
        )
        .filter(F.col(loop_count_col) > 0)
    )
