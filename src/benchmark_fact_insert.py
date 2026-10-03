"""
Phase 9: repeated before/after benchmark of the one step that was fixed, the fact insert.

Usage:
    python -m src.benchmark_fact_insert --repeats 3

Takes the fact rows for 1-7 January (726,002 rows) from data/processed/taxi.duckdb
(read-only) and loads them N times per variant into a FRESH on-disk database:
  before: fact_trip with the six FOREIGN KEY constraints (sql/03 as of Phases 2-8),
          loaded exactly as before: INSERT ... ON CONFLICT (trip_id) DO NOTHING
  after:  fact_trip without them, loaded by database.insert_fact_trips, which includes
          the set-based check_fact_foreign_keys that replaced them
Each timing includes a CHECKPOINT, so the index and constraint work is flushed to disk
inside the measurement. Writes results/phase9_fact_insert_benchmark.csv and .md.
"""
import argparse
import statistics
import time
from pathlib import Path

import duckdb
import pandas as pd

from src import database

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "results"
FK_CONSTRAINTS = ",\n".join(
    f"    CONSTRAINT fk_fact_trip_{column} FOREIGN KEY ({column}) REFERENCES {table} ({key})"
    for column, (table, key) in database.FACT_FOREIGN_KEYS.items()
)


def before_ddl() -> str:
    """sql/03 with the six FOREIGN KEY constraints put back, as it was in Phases 2-8."""
    return (database.SQL_DIR / "03_create_fact.sql").read_text(encoding="utf-8").replace(
        "CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id)",
        "CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id),\n" + FK_CONSTRAINTS)


def before_insert(con: duckdb.DuckDBPyConnection, rows: pd.DataFrame) -> None:
    con.register("fact_trip_batch", rows)
    columns = ", ".join(rows.columns)
    con.execute(f"INSERT INTO fact_trip ({columns}) SELECT {columns} FROM fact_trip_batch "
                f"ON CONFLICT (trip_id) DO NOTHING")
    con.unregister("fact_trip_batch")


def load_source(max_date_key: int) -> tuple:
    src = duckdb.connect(str(database.DEFAULT_DB_PATH), read_only=True)
    try:
        rows = src.execute(
            "SELECT * EXCLUDE (trip_key) FROM fact_trip WHERE date_key < ? ORDER BY trip_key",
            [max_date_key]).fetchdf()
        dims = {t: src.execute(f"SELECT * FROM {t}").fetchdf() for t in ["dim_date", "dim_location"]}
    finally:
        src.close()
    return rows, dims


def time_once(variant: str, rows: pd.DataFrame, dims: dict) -> float:
    db_path = PROJECT_ROOT / "data" / "processed" / f"bench_{variant}.duckdb"
    db_path.unlink(missing_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute((database.SQL_DIR / "02_create_dimensions.sql").read_text(encoding="utf-8"))
        for table, frame in dims.items():
            con.register("dim_batch", frame)
            con.execute(f"INSERT INTO {table} SELECT * FROM dim_batch")
            con.unregister("dim_batch")
        con.execute(before_ddl() if variant == "before"
                    else (database.SQL_DIR / "03_create_fact.sql").read_text(encoding="utf-8"))
        con.execute("CHECKPOINT")

        start = time.perf_counter()
        if variant == "before":
            before_insert(con, rows)
        else:
            database.insert_fact_trips(con, rows)
        con.execute("CHECKPOINT")
        seconds = time.perf_counter() - start

        assert database.fact_trip_count(con) == len(rows)
        return seconds
    finally:
        con.close()
        db_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Repeated before/after timing of the fact insert.")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--max-date-key", type=int, default=20260108,
                        help="Load fact rows with date_key below this (default: 1-7 Jan)")
    args = parser.parse_args()

    rows, dims = load_source(args.max_date_key)
    results = []
    for repeat in range(1, args.repeats + 1):
        for variant in ["before", "after"]:  # interleaved, so drift hits both equally
            seconds = time_once(variant, rows, dims)
            results.append({"variant": variant, "repeat": repeat, "seconds": round(seconds, 2)})
            print(f"repeat {repeat} {variant}: {seconds:.1f}s")

    table = pd.DataFrame(results)
    RESULTS_DIR.mkdir(exist_ok=True)
    table.to_csv(RESULTS_DIR / "phase9_fact_insert_benchmark.csv", index=False)
    med = table.groupby("variant")["seconds"].median()
    runs = table.groupby("variant")["seconds"].apply(lambda s: ", ".join(f"{v:.1f}" for v in s))
    lines = [
        "# Phase 9: fact insert, before vs after (repeated)",
        "",
        f"`python -m src.benchmark_fact_insert --repeats {args.repeats}`: {len(rows):,} fact rows "
        f"(date_key < {args.max_date_key}) loaded into a fresh on-disk database per run, with "
        f"variants interleaved. Each timing includes a CHECKPOINT.",
        "",
        "| Variant | Runs (s) | Median (s) | Rows/s (median) |",
        "|---|---|---:|---:|",
        *[f"| {v} | {runs[v]} | {med[v]:.1f} | {len(rows) / med[v]:,.0f} |" for v in ["before", "after"]],
        "",
        f"Speed-up (median): **{med['before'] / med['after']:.1f}x**. "
        f"Spread across runs: before {statistics.pstdev(table[table.variant == 'before'].seconds):.1f}s, "
        f"after {statistics.pstdev(table[table.variant == 'after'].seconds):.1f}s (population std dev).",
        "",
    ]
    (RESULTS_DIR / "phase9_fact_insert_benchmark.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
