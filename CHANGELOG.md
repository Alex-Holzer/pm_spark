# Changelog

All notable changes to **pm_spark** are summarized here. Optional local checklists
(`PROGRESS.md`, etc.) are gitignored for public GitHub — use this file for
release-facing history.

## [0.1.0] — 2026-03-22

### Added

- Initial public API: **eventlog** (preparation), **kpis** (metrics),
  **bottleneck** (duration), **variants** (analysis), **dimensions** (tracking).
- Flat re-exports and submodule aliases on `pm_spark` (`__all__`).
- **README** with Databricks install, module reference, and wheel build steps.
- **`dev/databricks_import_check.ipynb`** for post-install smoke on DBR.
- **`dev/smoke_import.py`** for local import + one-transform smoke (no pytest).

### Packaging

- Wheel via `pip wheel . --no-deps -w dist/`; **`dev/`** excluded from the wheel
  only (still tracked in git).
