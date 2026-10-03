"""
Unit 8 feature table: the grid is complete, every feature has the value its story promises,
splits follow the config, the leakage check passes on the real SQL and catches a
deliberately leaky feature, and snapshots are deterministic and immutable.
"""
import datetime as dt
import json
import os
import stat
from pathlib import Path

import pandas as pd
import pytest

from src import baseline, database, features, ingest

DAYS = 20
ZONES = [132, 236]


def make_trips() -> pd.DataFrame:
    """Zone 132 has `day` pickups at 08:xx on 1–20 Jan; zone 236 has one pickup at 17:00 every day."""
    pickups, zones = [], []
    for day in range(1, DAYS + 1):
        for i in range(day):
            pickups.append(pd.Timestamp(2026, 1, day, 8, i))
            zones.append(132)
        pickups.append(pd.Timestamp(2026, 1, day, 17, 0))
        zones.append(236)
    n = len(pickups)
    pickups = pd.to_datetime(pickups)
    return pd.DataFrame({
        "VendorID": [2] * n,
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": pickups + pd.Timedelta(minutes=15),
        "passenger_count": [1.0] * n,
        "trip_distance": [2.0] * n,
        "RatecodeID": [1.0] * n,
        "store_and_fwd_flag": ["N"] * n,
        "PULocationID": zones,
        "DOLocationID": [236] * n,
        "payment_type": [1] * n,
        "fare_amount": [12.0] * n,
        "extra": [0.0] * n,
        "mta_tax": [0.5] * n,
        "tip_amount": [2.0] * n,
        "tolls_amount": [0.0] * n,
        "improvement_surcharge": [1.0] * n,
        "total_amount": [15.5] * n,
        "congestion_surcharge": [0.0] * n,
        "Airport_fee": [0.0] * n,
        "cbd_congestion_fee": [0.0] * n,
    })


CONFIG = {
    "grid": {"start": dt.date(2026, 1, 1), "end": dt.date(2026, 1, DAYS)},
    "split": {"train_start": dt.date(2026, 1, 15), "validate_start": dt.date(2026, 1, 18),
              "test_start": dt.date(2026, 1, 19)},
    "calendar": {"public_holidays": [dt.date(2026, 1, 19)]},
    "reproducibility": {"seed": 42, "leakage_check_days": [dt.date(2026, 1, 10), dt.date(2026, 1, 16)]},
}


@pytest.fixture(scope="module")
def db_path(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("features")
    raw_dir = root / "raw"
    raw_dir.mkdir()
    make_trips().to_parquet(raw_dir / "yellow_tripdata_2026-01.parquet")
    ingest.run("2026-01", raw_dir, root / "taxi.duckdb", root / "logs")
    return root / "taxi.duckdb"


@pytest.fixture
def table(db_path) -> pd.DataFrame:
    con = database.get_connection(db_path)
    try:
        features.build_features(con, CONFIG)
        return con.execute("SELECT * FROM features_zone_hour").fetchdf()
    finally:
        con.close()


def row(table: pd.DataFrame, zone: int, day: int, hour: int) -> pd.Series:
    match = table[(table["zone_id"] == zone) & (table["hour_start"] == pd.Timestamp(2026, 1, day, hour))]
    assert len(match) == 1
    return match.iloc[0]


def test_grid_is_complete_and_zeros_are_explicit(table):
    assert len(table) == len(ZONES) * DAYS * 24
    assert table["target_trips"].sum() == len(make_trips())
    assert row(table, 132, 5, 3)["target_trips"] == 0  # no pickups at 03:00: a real 0, not a missing row


def test_history_features_match_their_stories(table):
    r = row(table, 132, 10, 8)
    assert r["target_trips"] == 10
    assert r["trips_same_hour_lag_1d"] == 9            # same hour yesterday
    assert r["trips_same_hour_lag_7d"] == 3            # same hour a week ago
    assert r["trips_same_hour_mean_7d"] == pytest.approx(6.0)  # mean of days 3..9
    assert r["zone_trips_prev_day"] == 9               # whole previous day
    assert not r["history_complete"]                   # only 9 days of history
    assert pd.isna(r["zone_trips_trend_7d"])

    r = row(table, 132, 15, 8)
    assert r["history_complete"]
    assert r["zone_trips_trend_7d"] == pytest.approx((sum(range(8, 15)) + 1) / (sum(range(1, 8)) + 1))

    assert pd.isna(row(table, 132, 1, 8)["trips_same_hour_lag_1d"])  # no history before the grid


def test_calendar_features(table):
    r = row(table, 236, 19, 17)  # Monday 19 Jan 2026, MLK Day
    assert r["day_of_week"] == 1 and not r["is_weekend"] and r["is_public_holiday"]
    assert row(table, 236, 17, 17)["is_weekend"]  # Saturday


def test_split_labels_follow_the_config(table):
    by_day = table.groupby("forecast_date")["split"].unique()
    assert all(len(labels) == 1 for labels in by_day)  # a day never straddles two splits
    labels = {pd.Timestamp(d).day: s[0] for d, s in by_day.items()}
    assert labels[14] == "warmup" and labels[15] == "train"
    assert labels[18] == "validate" and labels[19] == "test"


def test_leakage_check_passes_on_the_real_features(db_path):
    con = database.get_connection(db_path)
    try:
        assert features.check_no_future_leakage(con, CONFIG) == CONFIG["reproducibility"]["leakage_check_days"]
    finally:
        con.close()


def test_leakage_check_catches_a_leaky_feature(db_path, tmp_path, monkeypatch):
    # "Same hour yesterday" quietly replaced by "same hour today": the target itself.
    leaky_sql = tmp_path / "leaky.sql"
    leaky_sql.write_text(features.SQL_PATH.read_text(encoding="utf-8").replace(
        "LAG(trips, 1) OVER w AS trips_same_hour_lag_1d",
        "LAG(trips, 0) OVER w AS trips_same_hour_lag_1d"), encoding="utf-8")
    monkeypatch.setattr(features, "SQL_PATH", leaky_sql)

    con = database.get_connection(db_path)
    try:
        with pytest.raises(features.LeakageError, match="after the prediction moment"):
            features.check_no_future_leakage(con, CONFIG)
    finally:
        con.close()


def test_snapshot_is_deterministic_and_immutable(db_path, tmp_path):
    con = database.get_connection(db_path)
    try:
        features.build_features(con, CONFIG)
        first = features.write_snapshot(con, CONFIG, "vtest", root=tmp_path)
        parquet = tmp_path / "vtest" / first["file"]
        assert not os.stat(parquet).st_mode & stat.S_IWRITE  # read-only on disk

        features.build_features(con, CONFIG)  # rebuild from the same data: identical bytes
        assert features.write_snapshot(con, CONFIG, "vtest", root=tmp_path)["sha256"] == first["sha256"]

        changed = {**CONFIG, "calendar": {"public_holidays": []}}
        features.build_features(con, changed)
        with pytest.raises(FileExistsError, match="immutable"):
            features.write_snapshot(con, changed, "vtest", root=tmp_path)
    finally:
        con.close()

    manifest = json.loads((tmp_path / "vtest" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["rows"] == len(ZONES) * DAYS * 24
    assert manifest["rows_per_split"]["test"] == len(ZONES) * 2 * 24
    assert manifest["config"]["reproducibility"]["seed"] == 42


def test_baseline_profile_is_fitted_on_train_only():
    table = pd.DataFrame({
        "zone_id": [1, 1, 1], "hour_of_day": [8, 8, 8], "is_weekend": [False] * 3,
        "split": ["train", "train", "validate"], "target_trips": [2, 4, 100],
        "trips_same_hour_lag_7d": [None, None, 3], "trips_same_hour_lag_1d": [None, 2, 4],
        "trips_same_hour_mean_7d": [None, None, 3.0],
    })
    validate = table[table["split"] == "validate"]
    assert baseline.predict(table[table["split"] == "train"], validate)["train_profile"].iloc[0] == 3.0


def test_baseline_refuses_test_split_without_final(monkeypatch):
    monkeypatch.setattr("sys.argv", ["baseline", "--split", "test"])
    with pytest.raises(SystemExit):
        baseline.main()
