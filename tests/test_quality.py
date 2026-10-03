"""
Unit 6 quality checks: one test per check in src/validation.py, plus an end-to-end test
proving a deliberately bad row lands in quarantine_trip (with its reason codes), never
reaches fact_trip, and is quarantined only once when the month is replayed.
"""
import logging
from pathlib import Path

import duckdb
import pandas as pd

from src import database, ingest, transform, validation

MONTH = "2026-01"
NOW = pd.Timestamp("2026-10-03 12:00:00")


def make_rows(n: int = 3) -> pd.DataFrame:
    """n clean, distinct January trips that pass every hard and soft check."""
    pickups = pd.date_range("2026-01-10 08:00:00", periods=n, freq="h")
    return pd.DataFrame({
        "VendorID": [2] * n,
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": pickups + pd.Timedelta(minutes=20),
        "passenger_count": [1.0] * n,
        "trip_distance": [2.0 + i for i in range(n)],
        "RatecodeID": [1.0] * n,
        "store_and_fwd_flag": ["N"] * n,
        "PULocationID": [132] * n,
        "DOLocationID": [236] * n,
        "payment_type": [1] * n,
        "fare_amount": [12.0 + i for i in range(n)],
        "extra": [0.0] * n,
        "mta_tax": [0.5] * n,
        "tip_amount": [2.0] * n,
        "tolls_amount": [0.0] * n,
        "improvement_surcharge": [1.0] * n,
        "total_amount": [18.0 + i for i in range(n)],
        "congestion_surcharge": [2.5] * n,
        "Airport_fee": [0.0] * n,
        "cbd_congestion_fee": [0.0] * n,
    })


def reasons_for(df: pd.DataFrame) -> pd.Series:
    return validation.get_hard_reject_reasons(df, validation.build_check_context(month=MONTH, now=NOW))


def test_clean_rows_pass_every_check():
    df = make_rows()
    assert (reasons_for(df) == "").all()
    assert not validation.get_soft_anomaly_mask(df).any()


def test_every_check_is_tagged_with_a_quality_dimension():
    all_checks = set(validation.HARD_REJECT_CHECKS) | set(validation.SOFT_ANOMALY_CHECKS)
    assert set(validation.CHECK_DIMENSIONS) == all_checks
    assert set(validation.CHECK_DIMENSIONS.values()) == {"validity", "completeness", "uniqueness"}


def test_pickup_in_future_is_rejected():
    df = make_rows()
    df.loc[0, "tpep_pickup_datetime"] = pd.Timestamp("2030-01-01 08:00:00")
    df.loc[0, "tpep_dropoff_datetime"] = pd.Timestamp("2030-01-01 08:20:00")
    reasons = reasons_for(df)
    assert "pickup_in_future" in reasons.iloc[0]
    assert reasons.iloc[1] == ""


def test_pickup_outside_file_month_is_rejected():
    df = make_rows()
    df.loc[0, "tpep_pickup_datetime"] = pd.Timestamp("2025-12-31 23:50:00")
    df.loc[1, "tpep_pickup_datetime"] = pd.Timestamp("2026-02-01 00:00:00")
    df.loc[1, "tpep_dropoff_datetime"] = pd.Timestamp("2026-02-01 00:20:00")
    reasons = reasons_for(df)
    assert reasons.iloc[0] == "pickup_outside_file_month"
    assert reasons.iloc[1] == "pickup_outside_file_month"
    assert reasons.iloc[2] == ""


def test_pickup_month_check_is_skipped_without_a_month():
    df = make_rows()
    df.loc[0, "tpep_pickup_datetime"] = pd.Timestamp("2025-12-31 23:50:00")
    assert validation.get_hard_reject_reasons(df).iloc[0] == ""


def test_unknown_vendor_is_rejected():
    df = make_rows()
    df.loc[0, "VendorID"] = 99
    assert reasons_for(df).iloc[0] == "unknown_vendor"


def test_unknown_payment_type_is_rejected():
    df = make_rows()
    df.loc[0, "payment_type"] = 9
    df.loc[1, "payment_type"] = 0  # seeded "Unknown / not provided" member: valid
    reasons = reasons_for(df)
    assert reasons.iloc[0] == "unknown_payment_type"
    assert reasons.iloc[1] == ""


def test_unknown_rate_code_is_rejected_but_missing_rate_code_is_not():
    df = make_rows()
    df.loc[0, "RatecodeID"] = 42.0
    df.loc[1, "RatecodeID"] = None   # ~29% of January rows: completeness, not validity
    df.loc[2, "RatecodeID"] = 99.0   # seeded "Unknown / not provided" member: valid
    reasons = reasons_for(df)
    assert reasons.iloc[0] == "unknown_rate_code"
    assert reasons.iloc[1] == ""
    assert reasons.iloc[2] == ""


def test_valid_codes_come_from_the_check_context():
    df = make_rows()
    ctx = validation.build_check_context(month=MONTH, valid_vendor_ids={1}, now=NOW)
    assert (validation.get_hard_reject_reasons(df, ctx) == "unknown_vendor").all()


def test_location_id_out_of_range_is_rejected():
    df = make_rows()
    df.loc[0, "PULocationID"] = 0
    df.loc[1, "DOLocationID"] = 266
    df.loc[2, "DOLocationID"] = 265  # "Outside of NYC": valid
    reasons = reasons_for(df)
    assert reasons.iloc[0] == "location_id_out_of_range"
    assert reasons.iloc[1] == "location_id_out_of_range"
    assert reasons.iloc[2] == ""


def test_duplicate_in_batch_keeps_first_and_rejects_the_rest():
    df = make_rows(2)
    df = pd.concat([df, df.iloc[[0]], df.iloc[[0]]], ignore_index=True)
    reasons = reasons_for(df)
    assert list(reasons) == ["", "", "duplicate_in_batch", "duplicate_in_batch"]


def test_a_row_can_carry_several_reason_codes():
    df = make_rows()
    df.loc[0, "VendorID"] = 99
    df.loc[0, "PULocationID"] = 999
    reasons = reasons_for(df)
    assert reasons.iloc[0] == "unknown_vendor,location_id_out_of_range"
    assert validation.count_reasons(reasons) == {"unknown_vendor": 1, "location_id_out_of_range": 1}


def test_trip_over_24h_is_a_soft_anomaly_not_a_reject():
    df = make_rows()
    df.loc[0, "tpep_dropoff_datetime"] = df.loc[0, "tpep_pickup_datetime"] + pd.Timedelta(hours=25)
    assert reasons_for(df).iloc[0] == ""
    assert validation.get_soft_anomaly_mask(df).iloc[0]
    assert validation.get_soft_anomaly_counts(df)["trip_over_24h"] == 1


def test_completeness_warns_above_null_rate_threshold(caplog):
    df = make_rows(10)
    df.loc[:2, "passenger_count"] = None   # 30% null -> WARNING
    with caplog.at_level(logging.INFO):
        null_rates = validation.log_completeness(df, threshold=0.05)

    assert null_rates["passenger_count"] == 0.3
    assert null_rates["VendorID"] == 0.0
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "passenger_count" in warnings[0]


def test_bad_row_is_quarantined_once_and_never_loaded(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    df = make_rows(3)
    # The deliberately bad row: an unknown vendor, picked up in 2030.
    df.loc[1, "VendorID"] = 99
    df.loc[1, "tpep_pickup_datetime"] = pd.Timestamp("2030-01-01 09:00:00")
    df.loc[1, "tpep_dropoff_datetime"] = pd.Timestamp("2030-01-01 09:20:00")
    df.to_parquet(raw_dir / f"yellow_tripdata_{MONTH}.parquet")
    bad_trip_id = transform.add_trip_id(df).loc[1, "trip_id"]

    db_path = tmp_path / "taxi.duckdb"
    log_dir = tmp_path / "logs"

    first = ingest.run(MONTH, raw_dir, db_path, log_dir)
    second = ingest.run(MONTH, raw_dir, db_path, log_dir)

    assert first["rows_rejected"] == 1
    assert first["rows_quarantined"] == 1
    assert first["rows_inserted"] == 2
    assert first["reject_reason_counts"] == {
        "pickup_in_future": 1, "pickup_outside_file_month": 1, "unknown_vendor": 1,
    }
    assert second["rows_rejected"] == 1
    assert second["rows_quarantined"] == 0
    assert second["quarantine_trip_count"] == 1

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        quarantined = con.execute(
            "SELECT trip_id, VendorID, reason_codes, source_file, ingest_month FROM quarantine_trip"
        ).fetchall()
        in_fact = con.execute(
            "SELECT COUNT(*) FROM fact_trip WHERE trip_id = ?", [bad_trip_id]
        ).fetchone()[0]
    finally:
        con.close()

    assert quarantined == [(
        bad_trip_id, 99, "pickup_in_future,pickup_outside_file_month,unknown_vendor",
        f"yellow_tripdata_{MONTH}.parquet", MONTH,
    )]
    assert in_fact == 0

    rejected_csv = pd.read_csv(log_dir / f"rejected_{MONTH}.csv")
    assert list(rejected_csv["trip_id"]) == [bad_trip_id]


def test_existing_database_gains_quarantine_table_without_rebuild(tmp_path: Path):
    db_path = tmp_path / "taxi.duckdb"
    con = database.get_connection(db_path)
    database.ensure_schema(con)
    con.execute("DROP TABLE quarantine_trip")  # simulate a Phase 3-era database
    con.execute("INSERT INTO dim_date VALUES (20260101, '2026-01-01', 1, 1, 1, 2026, 3, 'Thursday')")

    database.ensure_schema(con)
    tables = {r[0] for r in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}
    dim_date_rows = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    con.close()

    assert "quarantine_trip" in tables
    assert dim_date_rows == 1  # existing data untouched
