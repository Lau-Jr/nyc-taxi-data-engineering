"""
Phase 9: profiles one full ingest run and writes the per-step timing table.

Usage:
    python -m src.profile_pipeline --month 2026-01 --label before

Runs the real pipeline (src/ingest.py) against a FRESH scratch database
(data/processed/profile_<label>.duckdb, deleted afterwards). Every profile therefore starts
from the same state and inserts the whole month, which makes before/after numbers
comparable. The real data/processed/taxi.duckdb is never touched. Writes
results/phase9_profile_<label>.csv and .md.
"""
import argparse
import platform
import shutil
import tempfile
import time
from pathlib import Path

import duckdb
import pandas as pd

from src import ingest

PROJECT_ROOT = ingest.PROJECT_ROOT
RESULTS_DIR = PROJECT_ROOT / "results"


def profile(month: str, label: str, raw_dir: Path) -> pd.DataFrame:
    db_path = PROJECT_ROOT / "data" / "processed" / f"profile_{label}.duckdb"
    db_path.unlink(missing_ok=True)
    log_dir = Path(tempfile.mkdtemp(prefix="profile_logs_"))
    try:
        start = time.perf_counter()
        summary = ingest.run(month, raw_dir, db_path, log_dir)
        wall = time.perf_counter() - start
    finally:
        db_path.unlink(missing_ok=True)
        shutil.rmtree(log_dir, ignore_errors=True)

    steps = pd.DataFrame(list(summary["step_seconds"].items()), columns=["step", "seconds"])
    other = wall - steps["seconds"].sum()
    steps.loc[len(steps)] = ["other (connections, pipeline_run, logging)", round(other, 2)]
    steps["share"] = steps["seconds"] / wall
    steps.attrs.update(wall=wall, rows_read=summary["rows_read"],
                       rows_inserted=summary["rows_inserted"], month=month, label=label)
    return steps


def write_results(steps: pd.DataFrame) -> Path:
    a = steps.attrs
    RESULTS_DIR.mkdir(exist_ok=True)
    steps.to_csv(RESULTS_DIR / f"phase9_profile_{a['label']}.csv", index=False)
    slowest = steps.loc[steps["seconds"].idxmax()]
    lines = [
        f"# Phase 9 profile: {a['label']}",
        "",
        f"`python -m src.profile_pipeline --month {a['month']} --label {a['label']}`: one full "
        f"ingest of {a['month']} ({a['rows_read']:,} rows read, {a['rows_inserted']:,} inserted) "
        f"into a fresh database. Run {pd.Timestamp.now():%Y-%m-%d %H:%M} on {platform.system()} "
        f"{platform.release()}, Python {platform.python_version()}, pandas {pd.__version__}, "
        f"DuckDB {duckdb.__version__}.",
        "",
        "| Step | Seconds | Share of run |",
        "|---|---:|---:|",
        *[f"| {'**' + r.step + '**' if r.step == slowest.step else r.step} | {r.seconds:,.1f} | {r.share:.1%} |"
          for r in steps.itertuples()],
        f"| **Total (wall clock)** | **{a['wall']:,.1f}** | 100% |",
        "",
        f"Slowest step: **{slowest.step}**, {slowest.seconds:,.1f} s ({slowest.share:.0%} of the run).",
        "",
    ]
    out = RESULTS_DIR / f"phase9_profile_{a['label']}.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile one full ingest run, step by step.")
    parser.add_argument("--month", default="2026-01")
    parser.add_argument("--label", required=True, help="e.g. before / after")
    parser.add_argument("--raw-dir", default=str(PROJECT_ROOT / "data" / "raw"))
    args = parser.parse_args()

    steps = profile(args.month, args.label, Path(args.raw_dir))
    out = write_results(steps)
    print(out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
