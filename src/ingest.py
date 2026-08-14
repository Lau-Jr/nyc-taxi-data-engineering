"""
Re-runnable ingestion pipeline: TLC yellow-taxi parquet -> validated DuckDB star schema.

Usage:
    python -m src.ingest --month 2026-01

Idempotency: fact_trip.trip_id is a deterministic SHA-256 of a fixed set of raw fields
(src/transform.py::TRIP_ID_FIELDS) with a UNIQUE constraint, loaded via
INSERT ... ON CONFLICT (trip_id) DO NOTHING. Running the same month twice inserts 0 new
rows the second time (see docs/phase3/idempotency_proof.md).
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

from src import database, transform, validation

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def configure_logging(month: str, log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ingestion_{month}.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    root.addHandler(file_handler)
    root.addHandler(stream_handler)


def write_rejected_rows(rejected_df: pd.DataFrame, reasons: pd.Series, log_dir: Path, month: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / f"rejected_{month}.csv"
    if rejected_df.empty:
        pd.DataFrame(columns=["trip_id", "reason"]).to_csv(out_path, index=False)
        return

    tagged = transform.add_trip_id(rejected_df)
    out = pd.DataFrame({"trip_id": tagged["trip_id"], "reason": reasons.loc[rejected_df.index]})
    out.to_csv(out_path, index=False)


def run(month: str, raw_dir: Path, db_path: Path, log_dir: Path) -> dict:
    configure_logging(month, log_dir)
    logging.info(f"Starting ingestion for month {month}")

    raw_path = raw_dir / f"yellow_tripdata_{month}.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    df = pd.read_parquet(raw_path)
    rows_read = len(df)
    logging.info(f"Rows read: {rows_read:,}")

    reasons = validation.get_hard_reject_reasons(df)
    reject_mask = reasons != ""
    rejected_df = df[reject_mask]
    valid_df = df[~reject_mask].copy()

    rows_rejected = len(rejected_df)
    rows_valid = len(valid_df)
    logging.info(f"Rows valid: {rows_valid:,}")
    logging.info(f"Rows rejected: {rows_rejected:,}")
    write_rejected_rows(rejected_df, reasons, log_dir, month)

    valid_df["is_anomaly"] = validation.get_soft_anomaly_mask(valid_df)
    anomaly_count = int(valid_df["is_anomaly"].sum())
    logging.info(f"Rows flagged as soft anomalies (loaded, not rejected): {anomaly_count:,}")

    valid_df = transform.add_trip_id(valid_df)
    valid_df = transform.add_date_key(valid_df)

    con = database.get_connection(db_path)
    try:
        database.ensure_schema(con)

        database.upsert_dim_date(con, transform.build_dim_date_rows(valid_df))
        database.upsert_dim_location(con, transform.build_dim_location_rows(valid_df))

        vendor_map = database.load_code_map(con, "dim_vendor", "vendor_id", "vendor_key")
        payment_map = database.load_code_map(con, "dim_payment", "payment_type_id", "payment_type_key")
        rate_map = database.load_code_map(con, "dim_rate_code", "rate_code_id", "rate_code_key")

        fact_rows = transform.prepare_fact_rows(valid_df, vendor_map, payment_map, rate_map)
        rows_inserted = database.insert_fact_trips(con, fact_rows)
        rows_skipped = rows_valid - rows_inserted
        total_fact_rows = database.fact_trip_count(con)
    finally:
        con.close()

    logging.info(f"Rows inserted: {rows_inserted:,}")
    logging.info(f"Rows skipped (already present): {rows_skipped:,}")
    logging.info(f"fact_trip row count after run: {total_fact_rows:,}")
    logging.info("Ingestion completed")

    return {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rows_inserted": rows_inserted,
        "rows_skipped": rows_skipped,
        "fact_trip_count": total_fact_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a month of NYC yellow taxi trip data.")
    parser.add_argument("--month", required=True, help="Month to ingest, format YYYY-MM (e.g. 2026-01)")
    parser.add_argument("--raw-dir", default=str(PROJECT_ROOT / "data" / "raw"))
    parser.add_argument("--db-path", default=str(database.DEFAULT_DB_PATH))
    parser.add_argument("--log-dir", default=str(PROJECT_ROOT / "logs"))
    args = parser.parse_args()

    run(args.month, Path(args.raw_dir), Path(args.db_path), Path(args.log_dir))


if __name__ == "__main__":
    main()
