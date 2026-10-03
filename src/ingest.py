"""
Re-runnable ingestion pipeline: TLC yellow-taxi parquet -> validated DuckDB star schema.

Usage:
    python -m src.ingest --month 2026-01

Idempotency: fact_trip.trip_id is a deterministic SHA-256 of a fixed set of raw fields
(src/transform.py::TRIP_ID_FIELDS) with a UNIQUE constraint, loaded via
INSERT ... ON CONFLICT (trip_id) DO NOTHING. Running the same month twice inserts 0 new
rows the second time (see docs/phase3/idempotency_proof.md).

Quality gate: rows failing a hard check (src/validation.py::HARD_REJECT_CHECKS) go to
quarantine_trip with their reason codes, the same way (ON CONFLICT (trip_id) DO NOTHING),
and to logs/rejected_<month>.csv (see docs/phase6/quarantine_proof.md).

Serving: every successful run rebuilds mart_daily_zone_trips (sql/07), and every run,
successful or not, is recorded in pipeline_run (see docs/phase7/serving_proof.md).
"""
import argparse
import logging
import time
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from src import database, transform, validation

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class StepTimer:
    """
    Times each pipeline step (Unit 9: measure before you tune). Every run logs one
    "[timing]" line per step, and the summary carries step_seconds, so the profile is
    always available, not only when someone remembers to measure.
    """

    def __init__(self) -> None:
        self.seconds = {}

    @contextmanager
    def step(self, name: str):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.seconds[name] = round(time.perf_counter() - start, 2)
            logging.info(f"[timing] {name}: {self.seconds[name]:.2f}s")

    def summary(self) -> str:
        total = sum(self.seconds.values())
        return ", ".join(f"{k}={v:.1f}s" for k, v in self.seconds.items()) + f" (total {total:.1f}s)"


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
    """Writes trip_id + reason for every hard-rejected row. rejected_df must carry trip_id."""
    log_dir.mkdir(parents=True, exist_ok=True)
    out_path = log_dir / f"rejected_{month}.csv"
    if rejected_df.empty:
        pd.DataFrame(columns=["trip_id", "reason"]).to_csv(out_path, index=False)
        return

    out = pd.DataFrame({"trip_id": rejected_df["trip_id"], "reason": reasons.loc[rejected_df.index]})
    out.to_csv(out_path, index=False)


def build_quarantine_rows(rejected_df: pd.DataFrame, reasons: pd.Series, raw_columns: list,
                          source_file: str, month: str) -> pd.DataFrame:
    """Shapes hard-rejected rows for quarantine_trip: raw columns + trip_id + reason/provenance."""
    rows = rejected_df[raw_columns + ["trip_id"]].copy()
    rows["reason_codes"] = reasons.loc[rejected_df.index]
    rows["source_file"] = source_file
    rows["ingest_month"] = month
    # Two rejected rows can share a trip_id (e.g. a row and its duplicate_in_batch copy):
    # quarantine the first, which is also what UNIQUE (trip_id) would keep.
    return rows.drop_duplicates(subset="trip_id", keep="first")


def run(month: str, raw_dir: Path, db_path: Path, log_dir: Path) -> dict:
    """
    Runs one ingest and records it in pipeline_run: 'running' at the start, then 'success'
    or 'failed' (with the error). The dashboard's freshness label reads that table, so a
    failed run is visible to consumers, not just in the log.
    """
    configure_logging(month, log_dir)
    logging.info(f"Starting ingestion for month {month}")

    con = database.get_connection(db_path)
    try:
        database.ensure_schema(con)
        run_id = database.start_pipeline_run(con, month, pd.Timestamp.now())
    finally:
        con.close()

    try:
        summary = _ingest(month, raw_dir, db_path, log_dir)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logging.error(f"Ingestion FAILED (pipeline_run {run_id}): {error}")
        con = database.get_connection(db_path)
        try:
            database.finish_pipeline_run(con, run_id, "failed", pd.Timestamp.now(), error=error)
        finally:
            con.close()
        raise

    con = database.get_connection(db_path)
    try:
        database.finish_pipeline_run(
            con, run_id, "success", pd.Timestamp.now(),
            rows_inserted=summary["rows_inserted"], mart_rows=summary["mart_rows"],
        )
    finally:
        con.close()
    summary["run_id"] = run_id
    return summary


def _ingest(month: str, raw_dir: Path, db_path: Path, log_dir: Path) -> dict:
    raw_path = raw_dir / f"yellow_tripdata_{month}.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    timer = StepTimer()
    with timer.step("read_parquet"):
        df = pd.read_parquet(raw_path)
    raw_columns = list(df.columns)
    rows_read = len(df)
    logging.info(f"Rows read: {rows_read:,}")

    with timer.step("completeness_log"):
        validation.log_completeness(df)
    with timer.step("add_trip_id"):
        df = transform.add_trip_id(df)

    con = database.get_connection(db_path)
    try:
        with timer.step("connect_and_load_code_maps"):
            database.ensure_schema(con)
            vendor_map = database.load_code_map(con, "dim_vendor", "vendor_id", "vendor_key")
            payment_map = database.load_code_map(con, "dim_payment", "payment_type_id", "payment_type_key")
            rate_map = database.load_code_map(con, "dim_rate_code", "rate_code_id", "rate_code_key")

        with timer.step("hard_checks"):
            # Valid codes come from the seeded dims, so a code added to sql/02 is accepted here too.
            check_ctx = validation.build_check_context(
                month=month,
                valid_vendor_ids=vendor_map.keys(),
                valid_payment_types=payment_map.keys(),
                valid_rate_codes=rate_map.keys(),
            )
            reasons = validation.get_hard_reject_reasons(df, check_ctx)
            reject_mask = reasons != ""
            rejected_df = df[reject_mask]
            valid_df = df[~reject_mask].copy()

        rows_rejected = len(rejected_df)
        rows_valid = len(valid_df)
        logging.info(f"Rows valid: {rows_valid:,}")
        logging.info(f"Rows rejected: {rows_rejected:,}")
        reason_counts = validation.count_reasons(reasons)
        for reason, count in reason_counts.items():
            logging.warning(
                f"Quarantined [{validation.CHECK_DIMENSIONS[reason]}] {reason}: {count:,} rows"
            )

        with timer.step("quarantine_write"):
            write_rejected_rows(rejected_df, reasons, log_dir, month)
            quarantine_rows = build_quarantine_rows(rejected_df, reasons, raw_columns, raw_path.name, month)
            rows_quarantined = database.insert_quarantine_rows(con, quarantine_rows)
            total_quarantine_rows = database.quarantine_trip_count(con)

        with timer.step("soft_checks"):
            valid_df["is_anomaly"] = validation.get_soft_anomaly_mask(valid_df)
            anomaly_count = int(valid_df["is_anomaly"].sum())
            soft_counts = validation.get_soft_anomaly_counts(valid_df)
        logging.info(f"Rows flagged as soft anomalies (loaded, not rejected): {anomaly_count:,}")
        for check_name, count in soft_counts.items():
            logging.info(f"  soft anomaly {check_name}: {count:,} rows")

        with timer.step("dims_upsert"):
            valid_df = transform.add_date_key(valid_df)
            database.upsert_dim_date(con, transform.build_dim_date_rows(valid_df))
            database.upsert_dim_location(con, transform.build_dim_location_rows(valid_df))

        with timer.step("prepare_fact_rows"):
            fact_rows = transform.prepare_fact_rows(valid_df, vendor_map, payment_map, rate_map)
        with timer.step("fact_insert"):
            rows_inserted = database.insert_fact_trips(con, fact_rows)
        rows_skipped = rows_valid - rows_inserted
        total_fact_rows = database.fact_trip_count(con)

        # Serving layer: rebuild the published mart from the full fact table (sql/07).
        with timer.step("mart_refresh"):
            mart_rows = database.refresh_marts(con, pd.Timestamp.now())
    finally:
        con.close()

    logging.info(f"Rows quarantined (new this run): {rows_quarantined:,}")
    logging.info(f"quarantine_trip row count after run: {total_quarantine_rows:,}")
    logging.info(f"Rows inserted: {rows_inserted:,}")
    logging.info(f"Rows skipped (already present): {rows_skipped:,}")
    logging.info(f"fact_trip row count after run: {total_fact_rows:,}")
    logging.info(f"mart_daily_zone_trips refreshed: {mart_rows:,} rows")
    logging.info(f"Step timings: {timer.summary()}")
    logging.info("Ingestion completed")

    return {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "rows_rejected": rows_rejected,
        "rows_quarantined": rows_quarantined,
        "reject_reason_counts": reason_counts,
        "rows_inserted": rows_inserted,
        "rows_skipped": rows_skipped,
        "fact_trip_count": total_fact_rows,
        "quarantine_trip_count": total_quarantine_rows,
        "mart_rows": mart_rows,
        "step_seconds": timer.seconds,
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
