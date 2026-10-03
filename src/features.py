"""
Phase 8: builds the ML feature table features_zone_hour (sql/08) from fact_trip, checks it
for future leakage, and writes an immutable, dated snapshot for modelling.

Usage:
    python -m src.features                  # build + leakage check + snapshot v<today>
    python -m src.features --version v2026-10-03

One command rebuilds the table from the warehouse; nothing is edited by hand. Settings
(grid dates, split boundaries, holidays, seed) come from config/features.toml and are
copied into the snapshot's manifest.json.
"""
import argparse
import hashlib
import json
import os
import stat
import subprocess
import tomllib
from pathlib import Path

import duckdb
import pandas as pd

from src import database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "features.toml"
SNAPSHOT_ROOT = PROJECT_ROOT / "data" / "features"
SQL_PATH = database.SQL_DIR / "08_build_features_zone_hour.sql"

FEATURE_TABLE = "features_zone_hour"
KEY_COLUMNS = ["zone_id", "hour_start", "forecast_date", "prediction_made_at"]
FEATURE_COLUMNS = [
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_public_holiday",
    "trips_same_hour_lag_1d",
    "trips_same_hour_lag_7d",
    "trips_same_hour_mean_7d",
    "zone_trips_prev_day",
    "zone_trips_trend_7d",
    "history_complete",
]
TARGET_COLUMN = "target_trips"
SPLIT_COLUMN = "split"

# Used as $cutoff for a normal build: every trip in fact_trip is visible.
NO_CUTOFF = pd.Timestamp("2100-01-01")


class LeakageError(AssertionError):
    """A feature changed when the data after its prediction moment was removed."""


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def build_features(con: duckdb.DuckDBPyConnection, config: dict,
                   cutoff: pd.Timestamp = NO_CUTOFF) -> int:
    """(Re)builds features_zone_hour, reading only trips picked up before `cutoff`."""
    con.execute(SQL_PATH.read_text(encoding="utf-8"), {
        "grid_start": config["grid"]["start"],
        "grid_end": config["grid"]["end"],
        "cutoff": cutoff.to_pydatetime(),
        "holidays": list(config["calendar"]["public_holidays"]),
        "train_start": config["split"]["train_start"],
        "validate_start": config["split"]["validate_start"],
        "test_start": config["split"]["test_start"],
    })
    return con.execute(f"SELECT COUNT(*) FROM {FEATURE_TABLE}").fetchone()[0]


def _day_features(con: duckdb.DuckDBPyConnection, day) -> pd.DataFrame:
    columns = ", ".join(KEY_COLUMNS + FEATURE_COLUMNS)
    return con.execute(
        f"SELECT {columns} FROM {FEATURE_TABLE} WHERE forecast_date = ? ORDER BY zone_id, hour_start",
        [day],
    ).fetchdf()


def check_no_future_leakage(con: duckdb.DuckDBPyConnection, config: dict) -> list:
    """
    The leakage question as code. For each check day D, rebuild the table from only the
    trips picked up before 00:00 on D (the prediction moment), and require every feature
    for day D to be identical to the full build. A feature that reads anything from D or
    later changes value and fails this check. The target is excluded: it is meant to be
    unknown at midnight.

    Leaves features_zone_hour in its full (uncut) state. Returns the days checked.
    """
    days = list(config["reproducibility"]["leakage_check_days"])
    truncated = {}
    for day in days:
        build_features(con, config, cutoff=pd.Timestamp(day))
        truncated[day] = _day_features(con, day)

    build_features(con, config)
    for day in days:
        full = _day_features(con, day)
        if full.empty:
            raise LeakageError(f"Leakage check day {day} is outside the feature grid")
        try:
            pd.testing.assert_frame_equal(full, truncated[day])
        except AssertionError as exc:
            raise LeakageError(
                f"Features for {day} change when trips from {day} onward are removed: "
                f"some feature reads data from after the prediction moment.\n{exc}"
            ) from exc
    return days


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git_commit() -> str:
    try:
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT,
                                capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=PROJECT_ROOT,
                               capture_output=True, text=True, check=True).stdout.strip()
        return commit + ("-dirty" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _export(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.execute(
        f"COPY (SELECT * FROM {FEATURE_TABLE} ORDER BY zone_id, hour_start) "
        f"TO '{path.as_posix()}' (FORMAT parquet)"
    )


def write_snapshot(con: duckdb.DuckDBPyConnection, config: dict, version: str,
                   root: Path = SNAPSHOT_ROOT, leakage_days: list = None) -> dict:
    """
    Writes data/features/<version>/features_zone_hour.parquet plus manifest.json, and
    marks the parquet read-only. A snapshot is never edited in place: if <version> already
    exists, an identical rebuild is a no-op and a different one is refused (choose a new
    version instead). Returns the manifest.
    """
    out_dir = root / version
    parquet_path = out_dir / f"{FEATURE_TABLE}.parquet"
    manifest_path = out_dir / "manifest.json"

    if parquet_path.exists():
        candidate = out_dir / f".{FEATURE_TABLE}.candidate.parquet"
        _export(con, candidate)
        same = _sha256(candidate) == _sha256(parquet_path)
        candidate.unlink()
        if not same:
            raise FileExistsError(
                f"Snapshot {version} already exists with different contents. Snapshots are "
                f"immutable: rebuild under a new --version."
            )
        return json.loads(manifest_path.read_text(encoding="utf-8"))

    out_dir.mkdir(parents=True, exist_ok=True)
    _export(con, parquet_path)

    split_counts = dict(con.execute(
        f"SELECT {SPLIT_COLUMN}, COUNT(*) FROM {FEATURE_TABLE} GROUP BY 1 ORDER BY MIN(forecast_date)"
    ).fetchall())
    runs = con.execute(
        "SELECT run_id, month, CAST(finished_at AS VARCHAR) FROM pipeline_run "
        "WHERE status = 'success' ORDER BY run_id"
    ).fetchall()
    manifest = {
        "version": version,
        "table": FEATURE_TABLE,
        "file": parquet_path.name,
        "sha256": _sha256(parquet_path),
        "rows": sum(split_counts.values()),
        "rows_per_split": split_counts,
        "zones": con.execute(f"SELECT COUNT(DISTINCT zone_id) FROM {FEATURE_TABLE}").fetchone()[0],
        "key_columns": KEY_COLUMNS,
        "feature_columns": FEATURE_COLUMNS,
        "target_column": TARGET_COLUMN,
        "split_column": SPLIT_COLUMN,
        "config": json.loads(json.dumps(config, default=str)),
        "source": {
            "fact_trip_rows": database.fact_trip_count(con),
            "successful_pipeline_runs": [
                {"run_id": r[0], "month": r[1], "finished_at": r[2]} for r in runs
            ],
        },
        "leakage_check_days": [str(d) for d in (leakage_days or [])],
        "git_commit": _git_commit(),
        "created_at": pd.Timestamp.now().isoformat(timespec="seconds"),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.chmod(parquet_path, stat.S_IREAD)
    return manifest


def run(db_path: Path, version: str, config_path: Path = CONFIG_PATH,
        root: Path = SNAPSHOT_ROOT) -> dict:
    config = load_config(config_path)
    con = database.get_connection(db_path)
    try:
        leakage_days = check_no_future_leakage(con, config)  # leaves the full build in place
        return write_snapshot(con, config, version, root, leakage_days)
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Phase 8 feature table and snapshot it.")
    parser.add_argument("--version", default=f"v{pd.Timestamp.now():%Y-%m-%d}",
                        help="Snapshot folder under data/features/ (default: v<today>)")
    parser.add_argument("--db-path", default=str(database.DEFAULT_DB_PATH))
    args = parser.parse_args()

    manifest = run(Path(args.db_path), args.version)
    print(f"features_zone_hour: {manifest['rows']:,} rows, {manifest['zones']} zones")
    print(f"rows per split: {manifest['rows_per_split']}")
    print(f"leakage check passed for: {', '.join(manifest['leakage_check_days'])}")
    print(f"snapshot: data/features/{manifest['version']}/{manifest['file']} (sha256 {manifest['sha256'][:12]}...)")


if __name__ == "__main__":
    main()
