# pm_spark

> 🧭 PySpark helpers for **process-mining** event logs — preparation, KPIs,
> duration and transition analysis, variant fingerprinting, and dimension
> tracking. Built for **large logs** and **Databricks**-style clusters.

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![PySpark 3.5.0](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org/docs/3.5.0/)
[![Databricks](https://img.shields.io/badge/Databricks-DBR%2014.3%20LTS-FF3621)](https://docs.databricks.com/)

---

## Contents

- 📦 [Installation and Databricks](#installation-and-databricks)
- 🧠 [Core concepts](#core-concepts)
  - [Uniform function signature](#uniform-function-signature)
  - [Event log contract](#event-log-contract)
  - [Chronological windows and tie-breaking](#chronological-windows-and-tie-breaking)
- 📚 [Module reference](#module-reference)
  - [Event log — `eventlog.preparation`](#event-log-eventlogpreparation)
  - [KPIs — `kpis.metrics`](#kpis-kpismetrics)
  - [Duration — `bottleneck.duration`](#duration-bottleneckduration)
  - [Variants — `variants.analysis`](#variants-variantsanalysis)
  - [Dimensions — `dimensions.tracking`](#dimensions-dimensionstracking)
- 💡 [Usage examples](#usage-examples)
- 🏗️ [Design decisions](#design-decisions)
- 🔧 [Building a new wheel](#building-a-new-wheel)
- 🤝 [Contributing](#contributing)
- 🗂️ [Project layout](#project-layout)
- 📜 [Further reading](#further-reading) (`CHANGELOG.md`)

---

## Installation and Databricks

### Requirements

| Piece | Detail |
|-------|--------|
| Python | **≥ 3.10** (library target: **3.10**, aligned with **DBR 14.3 LTS**) |
| PySpark | **3.5.0** (pinned in `pyproject.toml`; on Databricks the cluster already ships Spark) |

The wheel does **not** bundle Spark. On Databricks, prefer **`pip install … --no-deps`** so pip does not replace the cluster PySpark.

### Install from a wheel (typical for Databricks)

```bash
pip install /dbfs/path/to/pm_spark-0.1.0-py3-none-any.whl --no-deps
```

Use a path on **DBFS**, a **Unity Catalog volume**, or **workspace files**, depending on your org.

### Install from a Git checkout (local / editable)

This project uses **pip only** (no uv / poetry / conda):

```bash
cd /path/to/pm_spark
python3 -m pip install -e .
```

### Databricks: cluster library vs notebook `%pip`

- **Cluster → Libraries → Upload:** convenient, but confirm whether your workspace installs libraries **with** or **without** dependencies. If pip pulls **PySpark** from PyPI, use a **`%pip install /path/to.whl --no-deps`** cell or an **init script** with `--no-deps` instead.
- After `%pip`, use **Restart Python** (or reattach / restart per admin policy).

### Smoke test (import + one transform)

After install, run the cells in **`dev/databricks_import_check.ipynb`** (copy into your workspace) or:

```python
import pm_spark
from pm_spark import activity_sequence_number

print(pm_spark.__version__)
```

See also [Building a new wheel](#-building-a-new-wheel) for producing the `.whl`.

> **Note:** `dev/` is **excluded** from the wheel — clone the repo for notebooks and `dev/sample_data.py`.

---

## Core concepts

### Uniform function signature

Almost every public function follows the same leading arguments:

```text
fn(df, case_col, activity_col, timestamp_col, ...) -> DataFrame
```

(Some helpers return a **`float`** or a **`tuple`** of DataFrames; those are called out in the [module reference](#-module-reference).)

**Why `activity_col` (and sometimes `timestamp_col`) appear even when unused?**  
So pipelines stay **uniform**: you can chain with `DataFrame.transform` without reshuffling argument order. Unused parameters are intentional (often marked `# noqa: ARG001` in source).

### Event log contract

| Role | Column | Expectation |
|------|--------|-------------|
| Case | `case_col` | Process instance id. **Rows with null case keys are dropped** inside helpers. |
| Activity | `activity_col` | Step label (string-like). Null activities are excluded where docstrings say so (e.g. transitions). |
| Time | `timestamp_col` | **`TimestampType`** or **`TimestampNTZType`** — validated in many paths. |

Bring optional **business dimensions** as extra columns when using `dimensions.tracking`.

### Chronological windows and tie-breaking

Many functions order events **per case** by `timestamp_col`, then tie-breakers:

- By default, a **synthetic** stable key (e.g. `monotonically_increasing_id()`) may be used so ordering is deterministic when timestamps collide.
- Pass **`event_order_col`** when you have a source **sequence number** (recommended for production parity with source systems).
- For **reuse across several transforms**, build a window once with **`make_chronological_window`** and pass **`chronological_window=...`** into functions that support it.

**Exception:** **`activity_occurrence_index`** partitions by **`(case_col, activity_col)`**, so it **does not** accept an injected case-level `chronological_window` (PySpark `Window` objects are opaque). Use the **same** `event_order_col` you use elsewhere so ordering stays consistent. See [Design decisions](#activity_occurrence_index-and-chronological_window).

---

## Module reference

Import style (pick one):

```python
# Flat (re-exported from pm_spark)
from pm_spark import activity_sequence_number, throughput_time

# Explicit submodule (good for readability in large notebooks)
from pm_spark.eventlog import preparation as prep
from pm_spark.kpis import metrics as kpi
```

Below: **one line per function** — enough to choose the right tool; full parameter lists live in **docstrings** and `help(...)`.

### Event log (`eventlog.preparation`)

| Function | Returns | Key parameters / notes |
|----------|---------|-------------------------|
| `make_chronological_window` | `Window` | Positional **`*tiebreak_cols`** after the three core names (e.g. event id). |
| `detect_concurrency` | event-level `DataFrame` + cols | Flags **same timestamp** bursts per case (`activity_col` as array). |
| `activity_sequence_number` | event-level + 1 col | 1-based index per case; `chronological_window` or `event_order_col`. |
| `activity_occurrence_index` | event-level + 1 col | 1-based **per (case, activity)**; **no** `chronological_window` — use `event_order_col`. |
| `activity_position_ratio` | event-level + 1 col | Position in \([0,1]\) along case; single-event cases → `0.0`. |
| `case_start_timestamp` / `case_end_timestamp` | event-level + 1 col | Broadcast case extrema to every row. |
| `case_throughput_time` | event-level + 1 col | Case max−min time in **microseconds** (internal pattern: groupBy + broadcast). |
| `time_since_case_start` / `remaining_case_time` | event-level + 1 col | Microseconds from row time to case start / to case end. |
| `is_first_activity_flag` / `is_last_activity_flag` | event-level + 1 col | Boolean per row. |
| `first_occurrence_of_activity` / `last_occurrence_of_activity` | event-level + 1 col | Requires **`activity_name`** filter. |
| `duplicate_activity_flag` / `missing_activity_flag` | event-level + 1 col | Requires **`activity_name`**. |
| `event_count_per_case` | event-level + 1 col | Total events per case on every row. |
| `carry_forward_dimension_value` | event-level + 1 col | Forward-fill **`dimension_col`** within case. |
| `first_nonnull_in_dimension` / `last_nonnull_in_dimension` | event-level + 1 col | Broadcast non-null extrema per case. |
| `time_since_previous_event` | event-level + 1 col | **`LongType` microseconds** between consecutive events. |
| `previous_activity` / `next_activity` | event-level + 1 col | **Null** on first / last event in case order. |

### KPIs (`kpis.metrics`)

| Function | Returns | Key parameters / notes |
|----------|---------|-------------------------|
| `throughput_time` | **one row per case** | `unit`: seconds / minutes / hours / days; first→last timestamp. |
| `stp_rate` | **one row per case** | **`actor_col`**, **`technical_users`** (non-empty list), **`null_strategy`**: `exclude` \| `treat_as_technical`. |
| `rework_rate` | **one row per case** | Boolean: repeated **same** `activity_col` in case. |
| `activity_frequency_distribution` | **one row per activity** | **Not** one-row-per-case — global label frequencies + relative share. |
| `case_volume_over_time` | bucketed rows | Case-start volume by `period`: day / week / month. |
| `throughput_time_percentiles` | single summary row | e.g. P50 / P75 / P90 / P95 over per-case throughputs. |

### Duration (`bottleneck.duration`)

| Function | Returns | Key parameters / notes |
|----------|---------|-------------------------|
| `waiting_time_between_activities` | **`tuple`** `(event_df, summary_df)` | **`unit`**, **`summary_aggs`** e.g. `avg`/`sum`/`max`; optional **`tiebreak_col`**. |
| `time_between_activity_occurrences` | `DataFrame` | **`activity_a` / `activity_b`**, **`anchor_a` / `anchor_b`**: `first` \| `last`. |
| `activity_transition_matrix` | `DataFrame` | Directed **A → B** counts; optional **`tiebreak_col`**, **`sort_result`**. |
| `transition_time_matrix` | `DataFrame` | Count + **avg** + **median** duration per transition; **`accuracy`** for `percentile_approx`. |
| `longest_waiting_step_per_case` | **one row per case** | Max consecutive inter-event wait. |
| `time_to_first_occurrence_from_case_start` | event-level + 1 col | From case start to **first** hit of **`target_activity`**. |
| `parallel_activity_flag` | **one row per case** | **Same timestamp** appears on >1 row in case. |
| `activity_loop_detection` | **one row per case** | **`loop_count_col`**, **`loop_pairs_col`**; self / direct / indirect patterns. |

### Variants (`variants.analysis`)

| Function | Returns | Key parameters / notes |
|----------|---------|-------------------------|
| `case_variant_fingerprint` | **one row per case** | Ordered activity string; **`separator`**, optional **`event_order_col`**. |
| `variant_frequency_ranking` | variant-level table | Dense rank by frequency; optional **`fp_df`** to skip re-fingerprinting; **`max_variants`** guard. |
| `filter_top_n_variants` | event-level subset | Top **N** variants by rank; result **cached** — caller **`unpersist()`** when done. |
| `variant_coverage_ratio` | structured summary | Cumulative coverage vs rank; driver-side logic with explicit schema. |
| `variant_attribute_profile` | variant + attributes | Joins case attributes; duplicate-case guard. |
| `detect_variant_loops` | variant-level | Looping activities derived from fingerprint path. |
| `flag_rare_variants` | event-level + flag | Uses ranking; **cache** on return — **`unpersist()`** when finished. |
| `variant_drift_over_time` | period × variant | Fingerprint drift across **`period`** buckets; optional **`event_order_col`**. |

### Dimensions (`dimensions.tracking`)

| Function | Returns | Key parameters / notes |
|----------|---------|-------------------------|
| `distinct_nonnull_count_per_case` | event-level + 1 col | `countDistinct` semantics (nulls ignored). |
| `most_frequent_value_per_case` | event-level + 1 col | **`tiebreak_order`**: `lex` (string cast) vs `numeric`. |
| `nonnull_entry_count_per_case` | event-level + 1 col | Count of non-null dimension rows per case in order. |
| `dimension_stability_flag` | event-level + 1 col | **True** iff exactly **one** distinct non-null value per case. |
| `number_of_value_changes` | event-level + 1 col | Optional **`event_order_col`**. |
| `first_nonnull_value` / `last_nonnull_value` | event-level + 1 col | First/last non-null in case order; optional **`event_order_col`**. |
| `value_at_activity` | event-level + 1 col | Dimension value on rows where **`activity_col == activity_name`**. |
| `multi_value_case_rate` | **Python `float`** | Share of cases with **>1** distinct value; uses **`collect`** on aggregate. |
| `null_coverage_rate` | event-level + 2 cols | Per-case + **global** null share; **`count()`** materialization — caller may **`unpersist`**. |
| `dimension_value_transition_matrix` | transition rows | Consecutive **value → value** within case; optional **`drop_self_transitions`**. |

---

## Usage examples

### 1️⃣ Pipeline with `DataFrame.transform` (uniform signature)

PySpark **3.5** passes extra args after `df` into `transform`:

```python
from pm_spark.eventlog import preparation as prep

df_enriched = (
    df.transform(prep.activity_sequence_number, "case_id", "activity", "event_time")
    .transform(prep.previous_activity, "case_id", "activity", "event_time")
    .transform(prep.time_since_previous_event, "case_id", "activity", "event_time")
)
```

`throughput_time` aggregates to **one row per case**, so call it on the **event log** (enriched or raw is fine as long as case/time columns are consistent):

```python
from pm_spark import throughput_time

kpi_cases = throughput_time(
    df,
    case_col="case_id",
    activity_col="activity",
    timestamp_col="event_time",
    unit="days",
)
```

### 2️⃣ STP rate with explicit null handling

```python
from pm_spark import stp_rate

stp_cases = stp_rate(
    df,
    case_col="case_id",
    activity_col="activity",
    timestamp_col="event_time",
    actor_col="actor",
    technical_users=["system_user", "bot_01"],
    null_strategy="exclude",  # or "treat_as_technical"
)
```

### 3️⃣ Shared `Window` for preparation vs `tiebreak_col` for duration

`make_chronological_window` takes **positional** tie-break columns (not keyword `tiebreak_col`):

```python
from pm_spark.eventlog import preparation as prep

w = prep.make_chronological_window(
    "case_id",
    "activity",
    "event_time",
    "event_id",
)
df_seq = prep.activity_sequence_number(
    df,
    case_col="case_id",
    activity_col="activity",
    timestamp_col="event_time",
    chronological_window=w,
)
```

`bottleneck.duration` helpers take a **`tiebreak_col` name** (they build their own `Window` internally):

```python
from pm_spark.bottleneck import duration as dur

events, summary = dur.waiting_time_between_activities(
    df,
    case_col="case_id",
    activity_col="activity",
    timestamp_col="event_time",
    unit="hours",
    summary_aggs=["avg", "max"],
    tiebreak_col="event_id",
)
```

> ⚠️ **`waiting_time_between_activities`** is the **only** public API that returns **two** DataFrames. The first is **event-level** transitions; the second is **per-case** aggregates.

### 4️⃣ Variants: fingerprint then rank (reuse `fp_df`)

```python
from pm_spark.variants import analysis as va

df_fp = va.case_variant_fingerprint(
    df, "case_id", "activity", "event_time"
)
ranked = va.variant_frequency_ranking(
    df, "case_id", "activity", "event_time", fp_df=df_fp
)
```

---

## Design decisions

### Why `activity_col` appears in signatures that ignore it

**Composable pipelines** — especially `DataFrame.transform(fn, case_col, activity_col, timestamp_col, …)` — stay mechanically identical across your notebook. The small cost is extra parameters; the win is **no wrapper lambdas** and fewer ordering mistakes.

### activity_occurrence_index and chronological_window

That function needs **`Window.partitionBy(case_col, activity_col)`**. A case-only window from `make_chronological_window` cannot be re-mapped to that partition spec because **`Window` is opaque**. Pass **`event_order_col`** (same tie-break you use elsewhere) instead.

### `activity_frequency_distribution` is not one-row-per-case

It describes the **whole log** at **activity-label** granularity (counts + relative frequency). Do not expect the same grain as `throughput_time` / `stp_rate` / `rework_rate`.

### Null case keys

Helpers consistently **drop null `case_col`** via shared preparation (`_drop_null_case_keys`). You usually **do not** need to pre-filter unless your SLA requires different null semantics.

---

## Building a new wheel

1. Bump **`__version__`** in `pm_spark/__init__.py` (metadata reads it from `pyproject.toml`).
2. Use a **clean venv** (Python **3.10** recommended):

   ```bash
   python3.10 -m venv .venv && source .venv/bin/activate
   python -m pip install --upgrade pip setuptools wheel
   ```

3. Clean artifacts: `rm -rf dist/*.whl build/ *.egg-info`
4. From repo root:

   ```bash
   pip wheel . --no-deps -w dist/
   ```

5. Smoke-install:  
   `python -m pip install dist/pm_spark-<version>-py3-none-any.whl --no-deps`  
   then `python -c "import pm_spark; print(pm_spark.__version__)"`.

6. Optional local check **before** or **after** wheel install (editable tree on `PYTHONPATH`):

   ```bash
   python dev/smoke_import.py
   ```

Confirm **`dev/`** is absent inside the wheel (`unzip -l … | head`).

---

## Contributing

- **Tooling:** **pip** for installs; **no** uv / poetry / conda in this repo’s workflow.
- **Quality:** There is **no** automated test suite — validate with **`dev/sample_data.py`**, validation notebooks under **`dev/`**, **`python dev/smoke_import.py`** (quick import + one transform), and Databricks smoke runs.
- **API rule:** New **public** functions use  
  `(df, case_col, activity_col, timestamp_col, …) -> DataFrame`  
  (unless a deliberate exception such as a **`float`** KPI or **tuple** return — keep those rare and documented).
- **Style:** Match existing modules: **`F.col`**, named **`Window`** variables, DataFrame API only, Python **3.10** syntax ceiling.

See **`CHANGELOG.md`** for release bullets and **`pyproject.toml`** for packaging metadata. Internal progress checklists (if you use them) stay **local** — they are listed in **`.gitignore`** and are not pushed to GitHub. If those files were committed earlier, run  
`git rm --cached PROGRESS.md PROGRESS_ARCHIVE.md .cursorrules`  
(adjust paths) once, then commit.

---

## Project layout

```text
pm_spark/
├── pyproject.toml
├── README.md
├── CHANGELOG.md
├── pm_spark/
│   ├── __init__.py       # __version__, flat exports + submodule aliases
│   ├── _common.py        # shared validation / joins (internal)
│   ├── eventlog/preparation.py
│   ├── kpis/metrics.py
│   ├── bottleneck/duration.py
│   ├── variants/analysis.py
│   └── dimensions/tracking.py
└── dev/                  # tracked in git; excluded from wheel
    ├── sample_data.py
    ├── smoke_import.py
    ├── databricks_import_check.ipynb
    └── phase*_validation.ipynb
```

---

## Further reading

- **Docstrings:** `help(pm_spark.throughput_time)` or open the module in your IDE.
- **Releases:** `CHANGELOG.md` (bump when you change `__version__`).
- **Internal checklists:** optional `PROGRESS.md` / `PROGRESS_ARCHIVE.md` on your machine only (gitignored for public repos).
