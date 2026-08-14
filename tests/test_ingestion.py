from pathlib import Path

import pandas as pd
import pytest

from src import ingest, transform, validation


def make_sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "VendorID": [1, 1, 2, 2],
        "tpep_pickup_datetime": pd.to_datetime([
            "2026-01-05 08:00:00", "2026-01-05 09:00:00",
            "2026-01-06 10:00:00", "2026-01-06 11:00:00",
        ]),
        "tpep_dropoff_datetime": pd.to_datetime([
            "2026-01-05 08:15:00", "2026-01-05 08:50:00",  # row 1: dropoff before pickup
            "2026-01-06 10:20:00", "2026-01-06 11:30:00",
        ]),
        "passenger_count": [1.0, 1.0, 0.0, 2.0],
        "trip_distance": [2.5, 1.0, 0.0, 3.2],
        "RatecodeID": [1.0, 1.0, 1.0, None],
        "store_and_fwd_flag": ["N", "N", "N", "N"],
        "PULocationID": [100, 100, 150, None],
        "DOLocationID": [200, 200, 175, 175],
        "payment_type": [1, 1, 0, 2],
        "fare_amount": [10.0, 8.0, 5.0, -3.0],
        "extra": [0.5, 0.5, 0.0, 0.0],
        "mta_tax": [0.5, 0.5, 0.5, 0.5],
        "tip_amount": [2.0, 0.0, 0.0, 0.0],
        "tolls_amount": [0.0, 0.0, 0.0, 0.0],
        "improvement_surcharge": [0.3, 0.3, 0.3, 0.3],
        "total_amount": [13.3, 9.3, 5.8, -2.2],
        "congestion_surcharge": [2.5, 2.5, 2.5, 2.5],
        "Airport_fee": [0.0, 0.0, 0.0, 0.0],
        "cbd_congestion_fee": [0.0, 0.0, 0.0, 0.0],
    })


def test_trip_id_is_deterministic_and_unique_per_row():
    df = make_sample_df()
    tagged_a = transform.add_trip_id(df)
    tagged_b = transform.add_trip_id(df)

    assert (tagged_a["trip_id"] == tagged_b["trip_id"]).all()
    assert tagged_a["trip_id"].nunique() == len(df)


def test_trip_id_changes_when_a_key_field_changes():
    df = make_sample_df()
    changed = df.copy()
    changed.loc[0, "fare_amount"] = 999.0

    original_id = transform.add_trip_id(df).loc[0, "trip_id"]
    changed_id = transform.add_trip_id(changed).loc[0, "trip_id"]
    assert original_id != changed_id


def test_hard_reject_flags_dropoff_before_pickup_and_missing_location():
    df = make_sample_df()
    reasons = validation.get_hard_reject_reasons(df)

    assert "dropoff_before_or_equal_pickup" in reasons.iloc[1]
    assert "missing_pickup_location" in reasons.iloc[3]
    assert reasons.iloc[0] == ""


def test_soft_anomaly_flags_zero_distance_and_negative_amounts():
    df = make_sample_df()
    mask = validation.get_soft_anomaly_mask(df)

    assert mask.iloc[2]  # zero passenger_count, zero trip_distance
    assert mask.iloc[3]  # negative fare/total
    assert not mask.iloc[0]


def test_ingest_is_idempotent(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    df = make_sample_df()
    df.to_parquet(raw_dir / "yellow_tripdata_2026-01.parquet")

    db_path = tmp_path / "taxi.duckdb"
    log_dir = tmp_path / "logs"

    first = ingest.run("2026-01", raw_dir, db_path, log_dir)
    second = ingest.run("2026-01", raw_dir, db_path, log_dir)

    # Row 1 is hard-rejected (dropoff < pickup), row 3 has no pickup location -> also rejected.
    assert first["rows_read"] == 4
    assert first["rows_rejected"] == 2
    assert first["rows_valid"] == 2
    assert first["rows_inserted"] == 2
    assert first["rows_skipped"] == 0

    assert second["rows_inserted"] == 0
    assert second["rows_skipped"] == 2
    assert second["fact_trip_count"] == first["fact_trip_count"]
