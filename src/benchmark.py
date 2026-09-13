"""
Phase 4 benchmark: the same aggregate query run three ways (CSV + pandas, PostgreSQL,
DuckDB + Parquet) against the same month of raw trip data.

Usage:
    python -m src.benchmark prepare --month 2026-01
    python -m src.benchmark run --month 2026-01

PostgreSQL connection uses libpq's standard environment variables (PGHOST, PGPORT, PGUSER,
PGPASSWORD) — the password is never passed as a literal or written to any file here. Set it
in the shell before running, e.g. (PowerShell): $env:PGPASSWORD = '...'
"""
import argparse
import os
import time
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BENCHMARK_DIR = PROJECT_ROOT / "data" / "benchmark"
RESULTS_DIR = PROJECT_ROOT / "results"

PG_ADMIN_DB = "postgres"
PG_BENCH_DB = "nyc_taxi_benchmark"
PG_TABLE = "trips"

# Raw parquet column order -> flat Postgres table column order (COPY matches by position,
# not by name, so this order must match prepare_csv()'s CSV column order exactly).
PG_COLUMNS = [
    ("vendor_id", "INTEGER"),
    ("pickup_datetime", "TIMESTAMP"),
    ("dropoff_datetime", "TIMESTAMP"),
    ("passenger_count", "DOUBLE PRECISION"),
    ("trip_distance", "DOUBLE PRECISION"),
    ("rate_code_id", "DOUBLE PRECISION"),
    ("store_and_fwd_flag", "TEXT"),
    ("pu_location_id", "INTEGER"),
    ("do_location_id", "INTEGER"),
    ("payment_type", "INTEGER"),
    ("fare_amount", "DOUBLE PRECISION"),
    ("extra", "DOUBLE PRECISION"),
    ("mta_tax", "DOUBLE PRECISION"),
    ("tip_amount", "DOUBLE PRECISION"),
    ("tolls_amount", "DOUBLE PRECISION"),
    ("improvement_surcharge", "DOUBLE PRECISION"),
    ("total_amount", "DOUBLE PRECISION"),
    ("congestion_surcharge", "DOUBLE PRECISION"),
    ("airport_fee", "DOUBLE PRECISION"),
    ("cbd_congestion_fee", "DOUBLE PRECISION"),
]

N_RUNS = 3


def raw_parquet_path(month: str) -> Path:
    return RAW_DIR / f"yellow_tripdata_{month}.parquet"


def csv_path(month: str) -> Path:
    return BENCHMARK_DIR / f"yellow_tripdata_{month}.csv"


def pg_connect(dbname: str, autocommit: bool = False):
    """
    Connects using libpq's standard PG* environment variables for host/port/user/password —
    none of these are passed as literals here. Raises if PGPASSWORD (or another auth method
    libpq understands, e.g. .pgpass) isn't already set up in the environment.
    """
    conn = psycopg2.connect(dbname=dbname, host=os.environ.get("PGHOST", "127.0.0.1"),
                             port=os.environ.get("PGPORT", "5432"),
                             user=os.environ.get("PGUSER", "postgres"))
    conn.autocommit = autocommit
    return conn


def prepare_csv(month: str) -> Path:
    out_path = csv_path(month)
    if out_path.exists():
        print(f"[prepare] CSV already exists: {out_path}")
        return out_path

    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(raw_parquet_path(month))
    df.to_csv(out_path, index=False)
    print(f"[prepare] wrote {len(df):,} rows to {out_path}")
    return out_path


def prepare_postgres(month: str, csv_file: Path) -> float:
    """Creates the scratch database/table if needed and loads the CSV. Returns load seconds
    (0.0 if the table already had data and the load was skipped)."""
    admin_conn = pg_connect(PG_ADMIN_DB, autocommit=True)
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_BENCH_DB,))
            if cur.fetchone() is None:
                cur.execute(f"CREATE DATABASE {PG_BENCH_DB}")
                print(f"[prepare] created database {PG_BENCH_DB}")
    finally:
        admin_conn.close()

    bench_conn = pg_connect(PG_BENCH_DB, autocommit=True)
    try:
        with bench_conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (PG_TABLE,))
            table_exists = cur.fetchone()[0] is not None
            if table_exists:
                cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
                if cur.fetchone()[0] > 0:
                    print(f"[prepare] table {PG_TABLE} already loaded, skipping COPY")
                    return 0.0

            columns_sql = ", ".join(f"{name} {dtype}" for name, dtype in PG_COLUMNS)
            cur.execute(f"DROP TABLE IF EXISTS {PG_TABLE}")
            cur.execute(f"CREATE TABLE {PG_TABLE} ({columns_sql})")

            with open(csv_file, "r", encoding="utf-8") as f:
                start = time.perf_counter()
                cur.copy_expert(f"COPY {PG_TABLE} FROM STDIN WITH (FORMAT csv, HEADER)", f)
                load_seconds = time.perf_counter() - start

            cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
            row_count = cur.fetchone()[0]
            print(f"[prepare] loaded {row_count:,} rows into {PG_TABLE} in {load_seconds:.2f}s")
            return load_seconds
    finally:
        bench_conn.close()


def postgres_table_size_bytes() -> int:
    conn = pg_connect(PG_BENCH_DB, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size('{PG_TABLE}')")
            return cur.fetchone()[0]
    finally:
        conn.close()


def run_pandas(csv_file: Path) -> dict:
    cold_times = []
    df = None
    for _ in range(N_RUNS):
        start = time.perf_counter()
        df = pd.read_csv(csv_file, low_memory=False)
        result = (
            df.groupby("PULocationID")
            .agg(trip_count=("PULocationID", "size"), total_revenue=("total_amount", "sum"))
            .round(2)
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )
        cold_times.append(time.perf_counter() - start)

    warm_times = []
    for _ in range(N_RUNS):
        start = time.perf_counter()
        result = (
            df.groupby("PULocationID")
            .agg(trip_count=("PULocationID", "size"), total_revenue=("total_amount", "sum"))
            .round(2)
            .sort_values("total_revenue", ascending=False)
            .head(10)
        )
        warm_times.append(time.perf_counter() - start)

    top10 = {int(idx): float(row.total_revenue) for idx, row in result.iterrows()}
    return {"cold_times": cold_times, "warm_times": warm_times, "top10": top10}


PG_QUERY = f"""
    SELECT pu_location_id AS pickup_location_id,
           COUNT(*) AS trip_count,
           ROUND(SUM(total_amount)::numeric, 2) AS total_revenue
    FROM {PG_TABLE}
    GROUP BY pu_location_id
    ORDER BY total_revenue DESC
    LIMIT 10
"""


def run_postgres() -> dict:
    conn = pg_connect(PG_BENCH_DB, autocommit=True)
    times = []
    top10 = {}
    try:
        for _ in range(N_RUNS):
            with conn.cursor() as cur:
                start = time.perf_counter()
                cur.execute(PG_QUERY)
                rows = cur.fetchall()
                times.append(time.perf_counter() - start)
        top10 = {int(r[0]): float(r[2]) for r in rows}
    finally:
        conn.close()
    return {"times": times, "top10": top10}


def run_duckdb(month: str) -> dict:
    parquet_file = raw_parquet_path(month)
    query = f"""
        SELECT PULocationID AS pickup_location_id,
               COUNT(*) AS trip_count,
               ROUND(SUM(total_amount), 2) AS total_revenue
        FROM read_parquet('{parquet_file.as_posix()}')
        GROUP BY PULocationID
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    con = duckdb.connect(":memory:")
    times = []
    top10 = {}
    try:
        for _ in range(N_RUNS):
            start = time.perf_counter()
            rows = con.execute(query).fetchall()
            times.append(time.perf_counter() - start)
        top10 = {int(r[0]): float(r[2]) for r in rows}
    finally:
        con.close()
    return {"times": times, "top10": top10}


def cross_check(pandas_result: dict, postgres_result: dict, duckdb_result: dict) -> bool:
    """Confirms all three engines agree on the top-10 answer before any timing is trusted."""
    keys_match = (
        set(pandas_result["top10"]) == set(postgres_result["top10"]) == set(duckdb_result["top10"])
    )
    if not keys_match:
        return False
    return all(
        abs(pandas_result["top10"][k] - postgres_result["top10"][k]) < 0.01
        and abs(pandas_result["top10"][k] - duckdb_result["top10"][k]) < 0.01
        for k in pandas_result["top10"]
    )


def prepare(month: str) -> float:
    csv_file = prepare_csv(month)
    return prepare_postgres(month, csv_file)


def assemble_results(month: str, pandas_result: dict, postgres_result: dict, duckdb_result: dict,
                      pg_load_sec: float) -> pd.DataFrame:
    """Builds the results table shared by the CLI and the notebook, so both are guaranteed
    to report the same numbers from whichever benchmark run they were computed in."""
    csv_file = csv_path(month)
    parquet_file = raw_parquet_path(month)
    csv_size = csv_file.stat().st_size
    parquet_size = parquet_file.stat().st_size
    pg_size = postgres_table_size_bytes()

    rows = [
        {
            "engine": "pandas",
            "storage_format": "CSV",
            "source_path": str(csv_file.relative_to(PROJECT_ROOT)),
            "on_disk_size_mb": round(csv_size / (1024 * 1024), 2),
            "one_time_load_sec": "",
            "query_run1_sec": round(pandas_result["cold_times"][0], 4),
            "query_run2_sec": round(pandas_result["cold_times"][1], 4),
            "query_run3_sec": round(pandas_result["cold_times"][2], 4),
            "query_mean_sec": round(sum(pandas_result["cold_times"]) / N_RUNS, 4),
            "notes": "cold = read_csv + groupby together",
        },
        {
            "engine": "pandas",
            "storage_format": "CSV (in-memory)",
            "source_path": str(csv_file.relative_to(PROJECT_ROOT)),
            "on_disk_size_mb": round(csv_size / (1024 * 1024), 2),
            "one_time_load_sec": "",
            "query_run1_sec": round(pandas_result["warm_times"][0], 4),
            "query_run2_sec": round(pandas_result["warm_times"][1], 4),
            "query_run3_sec": round(pandas_result["warm_times"][2], 4),
            "query_mean_sec": round(sum(pandas_result["warm_times"]) / N_RUNS, 4),
            "notes": "warm = groupby only, CSV already loaded in memory",
        },
        {
            "engine": "PostgreSQL",
            "storage_format": "heap table",
            "source_path": f"{PG_BENCH_DB}.{PG_TABLE}",
            "on_disk_size_mb": round(pg_size / (1024 * 1024), 2),
            "one_time_load_sec": round(pg_load_sec, 2) if pg_load_sec else "0 (already loaded, COPY skipped)",
            "query_run1_sec": round(postgres_result["times"][0], 4),
            "query_run2_sec": round(postgres_result["times"][1], 4),
            "query_run3_sec": round(postgres_result["times"][2], 4),
            "query_mean_sec": round(sum(postgres_result["times"]) / N_RUNS, 4),
            "notes": "size = pg_total_relation_size (heap + MVCC overhead, not comparable byte-for-byte to a flat file)",
        },
        {
            "engine": "DuckDB",
            "storage_format": "Parquet",
            "source_path": str(parquet_file.relative_to(PROJECT_ROOT)),
            "on_disk_size_mb": round(parquet_size / (1024 * 1024), 2),
            "one_time_load_sec": "",
            "query_run1_sec": round(duckdb_result["times"][0], 4),
            "query_run2_sec": round(duckdb_result["times"][1], 4),
            "query_run3_sec": round(duckdb_result["times"][2], 4),
            "query_mean_sec": round(sum(duckdb_result["times"]) / N_RUNS, 4),
            "notes": "reads the compressed Parquet file directly, no load step",
        },
    ]

    return pd.DataFrame(rows)


def run(month: str) -> None:
    pg_load_sec = prepare(month)

    print("[run] pandas + CSV ...")
    pandas_result = run_pandas(csv_path(month))
    print("[run] PostgreSQL ...")
    postgres_result = run_postgres()
    print("[run] DuckDB + Parquet ...")
    duckdb_result = run_duckdb(month)

    ok = cross_check(pandas_result, postgres_result, duckdb_result)
    print(f"[run] cross-check (all three engines agree on top-10): {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise RuntimeError("Engines disagree on the query result — see cross_check output above.")

    out_df = assemble_results(month, pandas_result, postgres_result, duckdb_result, pg_load_sec)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(RESULTS_DIR / "benchmark_results.csv", index=False)
    print(f"[run] wrote {RESULTS_DIR / 'benchmark_results.csv'}")
    print(out_df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 4 benchmark: CSV+pandas vs PostgreSQL vs DuckDB+Parquet.")
    parser.add_argument("command", choices=["prepare", "run"])
    parser.add_argument("--month", default="2026-01")
    args = parser.parse_args()

    if args.command == "prepare":
        prepare(args.month)
    else:
        run(args.month)


if __name__ == "__main__":
    main()
