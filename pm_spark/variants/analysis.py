"""Variant fingerprinting, frequency ranking, coverage, drift, and filtering."""

import re
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pm_spark._common import _validate_columns


VALID_DRIFT_WINDOWS = frozenset({"week", "month", "quarter"})
RARE_MODE_ABSOLUTE = "absolute"
RARE_MODE_RELATIVE = "relative"
DEFAULT_MAX_VARIANTS = 100_000


def _resolve_fp_event_order(
    df: DataFrame,
    event_order_col: Optional[str],
) -> Tuple[DataFrame, F.Column]:
    """Returns DataFrame (with synthetic order column if needed) and order key.

    When ``event_order_col`` is None, adds ``__pm_fp_event_ord`` only for the
    aggregation subtree; it is not part of ``groupBy`` outputs.
    """

    if event_order_col is not None:
        _validate_columns(df, event_order_col)
        return df, F.col(event_order_col)
    tagged = df.withColumn("__pm_fp_event_ord", F.monotonically_increasing_id())
    return tagged, F.col("__pm_fp_event_ord")


def _per_case_fingerprints(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    separator: str,
    output_col: str,
    event_order_col: Optional[str] = None,
    with_case_start: bool = False,
) -> DataFrame:
    """Builds one row per case with ordered activity fingerprint string."""

    _validate_columns(df, case_col, activity_col, timestamp_col)
    df_ord, order_expr = _resolve_fp_event_order(df, event_order_col)
    df_valid = df_ord.filter(
        F.col(case_col).isNotNull() & F.col(activity_col).isNotNull()
    )
    struct_elem = F.struct(
        F.col(timestamp_col).alias("_ts"),
        order_expr.alias("_ord"),
        F.col(activity_col).alias("_act"),
    )
    sorted_structs = F.array_sort(F.collect_list(struct_elem))
    activity_array = F.transform(sorted_structs, lambda x: x.getField("_act"))
    fp_expr = F.concat_ws(separator, activity_array)
    aggs = [fp_expr.alias(output_col)]
    if with_case_start:
        aggs.append(F.min(F.col(timestamp_col)).alias("_case_start_ts"))
    return df_valid.groupBy(F.col(case_col)).agg(*aggs)


def _dataframe_from_variant_rank_rows(
    spark,
    sorted_rows: List,
    fingerprint_col: str,
    case_count_col: str,
    rank_col: str,
) -> DataFrame:
    """Builds a variant-level ranking DataFrame using driver-side dense ranks."""

    ranked_tuples = []
    prev_count = None
    dense_val = 0
    for row in sorted_rows:
        cnt = row[case_count_col]
        if prev_count is None or cnt != prev_count:
            dense_val += 1
            prev_count = cnt
        ranked_tuples.append((row[fingerprint_col], cnt, dense_val))
    return spark.createDataFrame(
        ranked_tuples,
        schema=[fingerprint_col, case_count_col, rank_col],
    )


def _variant_ranking_from_fp_work(
    fp_work: DataFrame,
    fingerprint_col: str,
    case_count_col: str,
    rank_col: str,
    max_variants: int,
) -> Tuple[DataFrame, float]:
    """Aggregates per-case fingerprints to variant counts and driver-side rank.

    Returns the ranked variant DataFrame and total case count (sum of counts).
    """

    counts = fp_work.groupBy(F.col(fingerprint_col)).agg(
        F.count(F.lit(1)).cast(T.LongType()).alias(case_count_col)
    )
    counts_cached = counts.cache()
    try:
        variant_n = counts_cached.count()
        if variant_n > max_variants:
            raise ValueError(
                "Distinct variant count %s exceeds max_variants=%s. "
                "Increase max_variants or reduce fingerprints before ranking."
                % (variant_n, max_variants)
            )
        collected = counts_cached.collect()
    finally:
        counts_cached.unpersist()

    total_cases = sum(int(r[case_count_col]) for r in collected)
    total_d = float(total_cases) if total_cases else 0.0
    sorted_rows = sorted(
        collected,
        key=lambda r: (-r[case_count_col], r[fingerprint_col]),
    )
    spark = fp_work.sparkSession
    ranked_df = _dataframe_from_variant_rank_rows(
        spark,
        sorted_rows,
        fingerprint_col,
        case_count_col,
        rank_col,
    )
    return ranked_df, total_d


def case_variant_fingerprint(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    separator: str = "→",
    output_col: str = "variant_fingerprint",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Collapses each case's ordered activity sequence into one string.

    Events are ordered by ``timestamp_col``, then ``event_order_col`` (or a
    synthetic monotonic id when ``event_order_col`` is None), then
    ``activity_col``. Rows with null case key or null activity are ignored for
    that case's sequence.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        separator: String placed between consecutive activity labels.
        output_col: Name of the fingerprint column.
        event_order_col: Optional stable per-event tie-breaker after timestamp.

    Returns:
        DataFrame with one row per distinct case and `output_col` as StringType.

    Example:
        >>> df_fp = case_variant_fingerprint(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     output_col="variant_fingerprint",
        ... )
    """

    return _per_case_fingerprints(
        df,
        case_col,
        activity_col,
        timestamp_col,
        separator,
        output_col,
        event_order_col=event_order_col,
    )


def variant_frequency_ranking(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    separator: str = "→",
    fingerprint_col: str = "variant_fingerprint",
    case_count_col: str = "case_count",
    rank_col: str = "variant_rank",
    event_order_col: Optional[str] = None,
    fp_df: Optional[DataFrame] = None,
    max_variants: int = DEFAULT_MAX_VARIANTS,
) -> DataFrame:
    """Counts cases per variant fingerprint and assigns a frequency rank.

    Fingerprints match ``case_variant_fingerprint()`` (including tie-break).
    Rank 1 is the most frequent variant; ties share the same dense rank
    (``dense_rank`` semantics), computed on the driver from the aggregated
    variant table so a single global shuffle is avoided.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        separator: Separator between activities in the fingerprint string.
        fingerprint_col: Column name for the variant fingerprint string.
        case_count_col: Column name for the number of cases per variant.
        rank_col: Column name for dense rank by descending case count.
        event_order_col: Optional per-event ordering column for fingerprinting.
        fp_df: Optional precomputed per-case fingerprints (one row per case);
            when set, fingerprinting on ``df`` is skipped and ``df`` is not read
            (only ``fp_df`` is validated).
        max_variants: Raise ``ValueError`` if distinct variants exceed this
            count (protects the driver-side ranking step).

    Returns:
        Variant-level DataFrame with fingerprint, case count, and rank columns.
        Output row order is undefined; use ``.orderBy(rank_col)`` for display.

    Example:
        >>> df_ranked = variant_frequency_ranking(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ... )
    """

    if fp_df is None:
        _validate_columns(df, case_col, activity_col, timestamp_col)
        fp_work = _per_case_fingerprints(
            df,
            case_col,
            activity_col,
            timestamp_col,
            separator,
            fingerprint_col,
            event_order_col=event_order_col,
        )
    else:
        _validate_columns(fp_df, case_col, fingerprint_col)
        fp_work = fp_df

    ranked_df, _ = _variant_ranking_from_fp_work(
        fp_work,
        fingerprint_col,
        case_count_col,
        rank_col,
        max_variants,
    )
    return ranked_df


def filter_top_n_variants(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    n: int,
    separator: str = "→",
    fingerprint_col: str = "variant_fingerprint",
    case_count_col: str = "case_count",
    rank_col: str = "variant_rank",
    event_order_col: Optional[str] = None,
    max_variants: int = DEFAULT_MAX_VARIANTS,
) -> DataFrame:
    """Returns event rows only for cases whose variant is in the top N by frequency.

    Ranking uses `variant_frequency_ranking()` (dense rank by case count).
    Variants with rank <= `n` are kept; the result is a left semi join of the
    original event log to those cases.

    Args:
        df: Input PySpark DataFrame containing the event log.
        case_col: Column name identifying the process case (case key).
        activity_col: Column name for the activity label.
        timestamp_col: Column name for the event timestamp.
        n: Number of top-ranked variants to include (by dense rank).
        separator: Separator between activities in the fingerprint string.
        fingerprint_col: Column name used for fingerprint matching.
        case_count_col: Column name from ranking (must match ranking output).
        rank_col: Column name for variant rank (must match ranking output).
        event_order_col: Optional per-event ordering column for fingerprinting.
        max_variants: Forwarded to `variant_frequency_ranking()`.

    Returns:
        Event-level DataFrame restricted to cases in the top N variants.
        The result is cached and materialized with ``count()`` before internal
        fingerprint storage is released; call ``unpersist()`` on the returned
        DataFrame when it is no longer needed.

    Example:
        >>> df_top = filter_top_n_variants(
        ...     df=event_log,
        ...     case_col="case_key",
        ...     activity_col="activity",
        ...     timestamp_col="timestamp",
        ...     n=3,
        ... )
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    if n < 1:
        raise ValueError("`n` must be a positive integer.")

    fp_per_case = _per_case_fingerprints(
        df,
        case_col,
        activity_col,
        timestamp_col,
        separator,
        fingerprint_col,
        event_order_col=event_order_col,
    )
    fp_per_case.cache()
    try:
        ranked = variant_frequency_ranking(
            df=df,
            case_col=case_col,
            activity_col=activity_col,
            timestamp_col=timestamp_col,
            separator=separator,
            fingerprint_col=fingerprint_col,
            case_count_col=case_count_col,
            rank_col=rank_col,
            event_order_col=event_order_col,
            fp_df=fp_per_case,
            max_variants=max_variants,
        )
        top_variants = ranked.filter(F.col(rank_col) <= F.lit(n)).select(
            F.col(fingerprint_col)
        )
        allowed_cases = fp_per_case.join(
            top_variants, on=fingerprint_col, how="left_semi"
        )
        result = df.join(
            allowed_cases.select(case_col), on=case_col, how="left_semi"
        )
        result.cache()
        result.count()
        return result
    finally:
        fp_per_case.unpersist()


def _split_fingerprint_to_array(fingerprint_column: str, separator: str) -> F.Column:
    """Splits a fingerprint string into an array using a regex-safe separator."""

    return F.split(F.col(fingerprint_column), re.escape(separator))


def variant_coverage_ratio(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    fingerprint_col: str = "variant_fingerprint",
    case_count_col: str = "case_count",
    rank_col: str = "variant_rank",
    cumulative_coverage_col: str = "cumulative_coverage",
    coverage_threshold: Optional[float] = None,
    max_variants: int = DEFAULT_MAX_VARIANTS,
) -> DataFrame:
    """Adds running cumulative case share over variant rank on a ranking DataFrame.

    Expects ``df`` shaped like ``variant_frequency_ranking()`` output (one row
    per variant). Cumulative share is computed on the driver from the variant
    table to avoid a global unpartitioned window and cross-joins.

    Args:
        df: Variant ranking DataFrame (fingerprint, case_count, rank).
        case_col: Unused; kept for uniform public API.
        activity_col: Unused; kept for uniform public API.
        timestamp_col: Unused; kept for uniform public API.
        fingerprint_col: Column holding the variant fingerprint / trace key.
        case_count_col: Column with per-variant case counts.
        rank_col: Dense rank column (must match ``dense_rank`` semantics).
        cumulative_coverage_col: Output column for cumulative share in [0, 1].
        coverage_threshold: If set, keep only variants with
            ``rank_col <= min(rank)`` where cumulative first reaches this share.
        max_variants: Raise ``ValueError`` if the variant row count exceeds this.

    Returns:
        Ranking DataFrame with ``cumulative_coverage_col`` and optional
        truncation. Output row order is undefined; use ``.orderBy(rank_col)``
        for display.
    """

    _validate_columns(df, fingerprint_col, case_count_col, rank_col)
    spark = df.sparkSession
    collected = df.collect()
    if len(collected) == 0:
        return df.withColumn(cumulative_coverage_col, F.lit(0.0))
    if len(collected) > max_variants:
        raise ValueError(
            "Variant row count %s exceeds max_variants=%s."
            % (len(collected), max_variants)
        )
    row_dicts = [r.asDict(recursive=True) for r in collected]
    row_dicts.sort(
        key=lambda d: (d[rank_col], -int(d[case_count_col])),
    )
    total_cases = sum(int(d[case_count_col]) for d in row_dicts)
    total_f = float(total_cases) if total_cases else 0.0
    if total_f <= 0.0:
        for d in row_dicts:
            d[cumulative_coverage_col] = 0.0
    else:
        running = 0.0
        for d in row_dicts:
            running += float(d[case_count_col])
            d[cumulative_coverage_col] = running / total_f

    if coverage_threshold is not None:
        eligible = [
            d[rank_col]
            for d in row_dicts
            if d[cumulative_coverage_col] >= coverage_threshold
        ]
        if eligible:
            cut_rank = min(eligible)
        else:
            cut_rank = max(d[rank_col] for d in row_dicts)
        row_dicts = [d for d in row_dicts if d[rank_col] <= cut_rank]

    out_schema = T.StructType(
        list(df.schema.fields)
        + [T.StructField(cumulative_coverage_col, T.DoubleType(), True)]
    )
    return spark.createDataFrame(row_dicts, schema=out_schema)


def variant_attribute_profile(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    attributes_df: DataFrame,
    attribute_join_col: str,
    fingerprint_col: str = "variant_fingerprint",
    continuous_cols: Optional[List[str]] = None,
    categorical_cols: Optional[List[str]] = None,
) -> DataFrame:
    """Aggregates case-level attributes by variant fingerprint.

    `df` must be exactly one row per case with `case_col` and `fingerprint_col`.
    Duplicate case keys raise ``ValueError`` (single aggregation pass; stops
    after the first duplicate key).
    Joins to `attributes_df` on ``df[case_col] == attributes[attribute_join_col]``.

    Args:
        df: Case-level DataFrame with variant fingerprint per case.
        case_col: Column identifying the case key.
        activity_col: Unused; kept for uniform public API.
        timestamp_col: Unused; kept for uniform public API.
        attributes_df: Case-level attributes (KPIs, dimensions).
        attribute_join_col: Join key column name on `attributes_df`.
        fingerprint_col: Variant fingerprint column on `df`.
        continuous_cols: Numeric columns to aggregate with ``avg`` (suffix ``_mean``).
        categorical_cols: Columns summarized with sorted distinct values
            (suffix ``_distinct_values`` as array<string>).

    Returns:
        One row per distinct `fingerprint_col` with requested aggregates.
    """

    _validate_columns(df, case_col, fingerprint_col)
    _validate_columns(attributes_df, attribute_join_col)
    continuous_cols = continuous_cols or []
    categorical_cols = categorical_cols or []
    overlap = set(continuous_cols) & set(categorical_cols)
    if overlap:
        raise ValueError(
            "Columns cannot be both continuous and categorical: %s" % sorted(overlap)
        )

    dupes = (
        df.groupBy(F.col(case_col))
        .agg(F.count(F.lit(1)).alias("_n"))
        .filter(F.col("_n") > 1)
        .limit(1)
        .count()
    )
    if dupes > 0:
        raise ValueError(
            "Input `df` must have one row per `%s`. At least one case key has "
            "multiple rows. Pass a case-level DataFrame (e.g. from "
            "case_variant_fingerprint())." % case_col
        )

    cases = df.select(F.col(case_col), F.col(fingerprint_col))
    attrs = (
        attributes_df
        if attribute_join_col == case_col
        else attributes_df.withColumnRenamed(attribute_join_col, case_col)
    )
    merged = cases.join(attrs, on=case_col, how="inner")
    for col_name in continuous_cols:
        _validate_columns(merged, col_name)
    for col_name in categorical_cols:
        _validate_columns(merged, col_name)

    agg_exprs = [F.count(F.lit(1)).cast(T.LongType()).alias("case_count_profile")]
    for col_name in continuous_cols:
        agg_exprs.append(F.avg(F.col(col_name)).alias(f"{col_name}_mean"))
    for col_name in categorical_cols:
        agg_exprs.append(
            F.array_sort(F.collect_set(F.col(col_name))).alias(
                f"{col_name}_distinct_values"
            )
        )

    return merged.groupBy(F.col(fingerprint_col)).agg(*agg_exprs)


def detect_variant_loops(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    fingerprint_col: str = "variant_fingerprint",
    separator: str = "→",
    min_repetitions: int = 2,
    has_loop_col: str = "has_loop",
    looping_activities_col: str = "looping_activities",
    include_looping_activities: bool = True,
) -> DataFrame:
    """Detects repeated activities inside a fingerprint string without the event log.

    Args:
        df: DataFrame containing `fingerprint_col` (e.g. per-case fingerprints).
        case_col: Unused; kept for uniform public API.
        activity_col: Unused; kept for uniform public API.
        timestamp_col: Unused; kept for uniform public API.
        fingerprint_col: Column with separator-joined activity sequence.
        separator: Separator used when the fingerprint was built.
        min_repetitions: Minimum occurrences of one activity to count as a loop.
        has_loop_col: Boolean output column name.
        looping_activities_col: Array column listing activities meeting threshold.
        include_looping_activities: If False, drops the array column from output.

    Returns:
        One row per distinct fingerprint with loop flags.
    """

    if min_repetitions < 2:
        raise ValueError("`min_repetitions` must be at least 2.")

    _validate_columns(df, fingerprint_col)
    fp_distinct = df.select(F.col(fingerprint_col)).distinct()
    split_col = _split_fingerprint_to_array(fingerprint_col, separator)
    exploded = fp_distinct.select(
        F.col(fingerprint_col),
        F.posexplode(split_col).alias("_pos", "_act"),
    ).filter(F.col("_act") != F.lit(""))

    counts = exploded.groupBy(F.col(fingerprint_col), F.col("_act")).agg(
        F.count(F.lit(1)).cast(T.LongType()).alias("_cnt")
    )
    repeats = counts.filter(F.col("_cnt") >= F.lit(min_repetitions))
    looping = repeats.groupBy(F.col(fingerprint_col)).agg(
        F.array_sort(F.collect_list(F.col("_act"))).alias(looping_activities_col)
    )
    out = fp_distinct.join(looping, on=fingerprint_col, how="left").withColumn(
        has_loop_col,
        F.coalesce(F.size(F.col(looping_activities_col)) > 0, F.lit(False)),
    )
    if not include_looping_activities:
        out = out.drop(looping_activities_col)
    return out


def flag_rare_variants(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    mode: str = RARE_MODE_ABSOLUTE,
    min_count: int = 5,
    min_share: Optional[float] = None,
    fingerprint_col: str = "variant_fingerprint",
    case_count_col: str = "case_count",
    is_rare_col: str = "is_rare_variant",
    separator: str = "→",
    event_order_col: Optional[str] = None,
    max_variants: int = DEFAULT_MAX_VARIANTS,
) -> DataFrame:
    """Flags each event row when its case's variant is rare vs global frequency.

    Args:
        df: Event-level event log.
        case_col: Case key column.
        activity_col: Activity label column.
        timestamp_col: Event timestamp column.
        mode: ``"absolute"`` (``case_count < min_count``) or
            ``"relative"`` (share of cases ``< min_share``).
        min_count: Threshold when ``mode="absolute"``.
        min_share: Required when ``mode="relative"`` (strictly between 0 and 1).
        fingerprint_col: Fingerprint column name used internally.
        case_count_col: Must match `variant_frequency_ranking()` output.
        is_rare_col: Output boolean column on the event log.
        separator: Fingerprint separator consistent with other variant helpers.
        event_order_col: Optional per-event ordering column for fingerprinting.
        max_variants: Forwarded to `variant_frequency_ranking()`.

    Returns:
        Event log with `is_rare_col` joined from per-case variant stats.
        The result is cached and materialized with ``count()`` before internal
        fingerprint storage is released; call ``unpersist()`` on the returned
        DataFrame when it is no longer needed.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    if mode not in {RARE_MODE_ABSOLUTE, RARE_MODE_RELATIVE}:
        raise ValueError("`mode` must be 'absolute' or 'relative'.")
    if mode == RARE_MODE_RELATIVE:
        if min_share is None:
            raise ValueError("`min_share` is required when mode='relative'.")
        if not (0.0 < min_share < 1.0):
            raise ValueError("`min_share` must be strictly between 0 and 1.")

    fp_case = _per_case_fingerprints(
        df,
        case_col,
        activity_col,
        timestamp_col,
        separator,
        fingerprint_col,
        event_order_col=event_order_col,
    )
    fp_case.cache()
    try:
        ranked, total_d = _variant_ranking_from_fp_work(
            fp_case,
            fingerprint_col,
            case_count_col,
            "variant_rank",
            max_variants,
        )
        stats = ranked.withColumn(
            "_coverage_share",
            F.when(
                F.lit(total_d) > 0.0,
                F.col(case_count_col) / F.lit(total_d),
            ).otherwise(F.lit(0.0)),
        )
        per_case = fp_case.join(stats, on=fingerprint_col, how="left")
        if mode == RARE_MODE_ABSOLUTE:
            flag_expr = F.col(case_count_col) < F.lit(min_count)
        else:
            flag_expr = F.col("_coverage_share") < F.lit(float(min_share))

        flagged = per_case.withColumn(is_rare_col, flag_expr).select(
            F.col(case_col),
            F.col(is_rare_col),
        )
        result = df.join(flagged, on=case_col, how="left")
        result.cache()
        result.count()
        return result
    finally:
        fp_case.unpersist()


def variant_drift_over_time(
    df: DataFrame,
    case_col: str,
    activity_col: str,
    timestamp_col: str,
    time_window: str = "month",
    separator: str = "→",
    fingerprint_col: str = "variant_fingerprint",
    case_count_col: str = "case_count",
    rank_col: str = "variant_rank",
    include_rank_change: bool = False,
    rank_change_col: str = "rank_change_vs_prior_period",
    event_order_col: Optional[str] = None,
) -> DataFrame:
    """Buckets cases by start period and ranks variants within each period.

    Case start is ``min(timestamp_col)`` per case (non-null activities only,
    matching fingerprinting). ``date_trunc`` uses Spark semantics (e.g.
    ``week`` follows Monday-based ISO-style bucketing).

    Args:
        df: Event-level event log.
        case_col: Case key column.
        activity_col: Activity label column.
        timestamp_col: Event timestamp column.
        time_window: One of ``week``, ``month``, ``quarter``.
        separator: Fingerprint separator.
        fingerprint_col: Fingerprint column name in internal joins.
        case_count_col: Output column for case counts per period and variant.
        rank_col: ``dense_rank()`` within each period by descending case count
            (same semantics as ``variant_frequency_ranking()``).
        include_rank_change: If True, adds lag of `rank_col` across periods
            per variant (difference vs prior period).
        rank_change_col: Name of the rank-change column.
        event_order_col: Optional per-event ordering column for fingerprinting.

    Returns:
        Long-format DataFrame:
        ``(period, fingerprint, case_count, variant_rank [, rank_change])``.
        Output row order is undefined; use ``.orderBy(period, rank_col)`` for
        display.
    """

    _validate_columns(df, case_col, activity_col, timestamp_col)
    normalized = time_window.lower()
    if normalized not in VALID_DRIFT_WINDOWS:
        raise ValueError("`time_window` must be one of: week, month, quarter.")

    df_valid = df.filter(
        F.col(case_col).isNotNull() & F.col(activity_col).isNotNull()
    )
    fp_case = _per_case_fingerprints(
        df_valid,
        case_col,
        activity_col,
        timestamp_col,
        separator,
        fingerprint_col,
        event_order_col=event_order_col,
        with_case_start=True,
    )
    period_col = "period"
    start_ts = F.col("_case_start_ts")
    with_period = fp_case.withColumn(
        period_col,
        F.date_trunc(normalized, start_ts),
    ).drop("_case_start_ts")
    counts = with_period.groupBy(F.col(period_col), F.col(fingerprint_col)).agg(
        F.count(F.lit(1)).cast(T.LongType()).alias(case_count_col)
    )
    w_period = Window.partitionBy(period_col).orderBy(F.col(case_count_col).desc())
    ranked = counts.withColumn(rank_col, F.dense_rank().over(w_period))
    if not include_rank_change:
        return ranked

    w_variant = Window.partitionBy(fingerprint_col).orderBy(F.col(period_col))
    prior = F.lag(F.col(rank_col), 1).over(w_variant)
    return ranked.withColumn(rank_change_col, F.col(rank_col) - prior)
