"""
Unit 7 serving layer: the published mart reconciles with fact_trip, its refresh is
idempotent, metrics roll up correctly, and the freshness label turns stale when the
pipeline fails, then recovers once a run succeeds.
"""
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from src import database, ingest, metrics, serving

APP_PATH = str(Path(__file__).resolve().parent.parent / "streamlit_app.py")


def make_month(month: str, n: int = 4) -> pd.DataFrame:
    """n clean trips on two days of `month`, over two pickup zones, one paid by card."""
    pickups = pd.to_datetime([f"{month}-10 08:00", f"{month}-10 09:00",
                              f"{month}-11 08:00", f"{month}-11 09:00"][:n])
    return pd.DataFrame({
        "VendorID": [2] * n,
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": pickups + pd.Timedelta(minutes=20),
        "passenger_count": [1.0] * n,
        "trip_distance": [2.0, 4.0, 3.0, 5.0][:n],
        "RatecodeID": [1.0] * n,
        "store_and_fwd_flag": ["N"] * n,
        "PULocationID": [132, 236, 132, 236][:n],
        "DOLocationID": [236] * n,
        "payment_type": [1, 2, 2, 2][:n],
        "fare_amount": [10.0, 20.0, 30.0, 40.0][:n],
        "extra": [0.0] * n,
        "mta_tax": [0.5] * n,
        "tip_amount": [2.0, 0.0, 0.0, 0.0][:n],
        "tolls_amount": [0.0] * n,
        "improvement_surcharge": [1.0] * n,
        "total_amount": [13.5, 21.5, 31.5, 41.5][:n],
        "congestion_surcharge": [0.0] * n,
        "Airport_fee": [0.0] * n,
        "cbd_congestion_fee": [0.0] * n,
    })


@pytest.fixture
def env(tmp_path: Path) -> dict:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    return {"raw_dir": raw_dir, "db_path": tmp_path / "taxi.duckdb", "log_dir": tmp_path / "logs"}


def write_month(env: dict, month: str, df: pd.DataFrame) -> None:
    df.to_parquet(env["raw_dir"] / f"yellow_tripdata_{month}.parquet")


def run(env: dict, month: str) -> dict:
    return ingest.run(month, env["raw_dir"], env["db_path"], env["log_dir"])


def read_only(env: dict) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(env["db_path"]), read_only=True)


def test_mart_reconciles_with_fact_trip(env):
    df = make_month("2026-01")
    df.loc[3, "passenger_count"] = None  # soft anomaly: still counted, flagged separately
    df.loc[2, "trip_distance"] = 5000.0  # odometer error: counted, but not in avg distance
    write_month(env, "2026-01", df)
    summary = run(env, "2026-01")

    con = read_only(env)
    try:
        fact_trips, fact_revenue = con.execute(
            "SELECT COUNT(*), SUM(total_amount) FROM fact_trip").fetchone()
        mart = serving.load_mart(con)
    finally:
        con.close()

    assert summary["mart_rows"] == len(mart) == 4  # 2 days x 2 zones
    assert mart["trip_count"].sum() == fact_trips == 4
    assert float(mart["total_revenue"].sum()) == pytest.approx(float(fact_revenue))
    assert mart["anomaly_trip_count"].sum() == 1
    assert metrics.summarise(mart).iloc[0]["avg_trip_distance"] == pytest.approx((2 + 4 + 5) / 3)


def test_mart_refresh_is_idempotent(env):
    write_month(env, "2026-01", make_month("2026-01"))
    run(env, "2026-01")
    con = database.get_connection(env["db_path"])
    try:
        before = serving.load_mart(con).drop(columns="refreshed_at")
        database.refresh_marts(con, pd.Timestamp.now())
        after = serving.load_mart(con).drop(columns="refreshed_at")
    finally:
        con.close()
    pd.testing.assert_frame_equal(before, after)


def test_metrics_roll_up_from_sums_not_averages():
    mart = pd.DataFrame({
        "pickup_date": ["d1", "d2"],
        "trip_count": [1, 3], "anomaly_trip_count": [0, 1], "total_revenue": [10.0, 90.0],
        "paid_trip_count": [1, 3], "paid_fare_sum": [10.0, 90.0],
        "distance_trip_count": [1, 3], "distance_sum": [1.0, 9.0],
        "card_fare_sum": [10.0, 0.0], "card_tip_sum": [2.0, 0.0],
    })
    total = metrics.summarise(mart).iloc[0]
    assert total["trip_count"] == 4
    assert total["avg_fare"] == 25.0          # 100 / 4, not mean(10, 30) = 20
    assert total["avg_trip_distance"] == 2.5
    assert total["card_tip_rate"] == 0.2
    assert total["anomaly_share"] == 0.25

    by_day = metrics.summarise(mart, by=["pickup_date"]).set_index("pickup_date")
    assert by_day.loc["d2", "avg_fare"] == 30.0
    assert pd.isna(by_day.loc["d2", "card_tip_rate"])  # no card fares: undefined, not 0


def test_failed_run_turns_freshness_stale_then_recovers(env):
    write_month(env, "2026-01", make_month("2026-01"))
    run(env, "2026-01")
    con = read_only(env)
    try:
        fresh = serving.get_freshness(con)
    finally:
        con.close()
    assert fresh["level"] == "fresh"
    assert fresh["data_as_of"] == pd.Timestamp("2026-01-11 09:00")

    # Break the refresh: February's file never arrived.
    with pytest.raises(FileNotFoundError):
        run(env, "2026-02")
    con = read_only(env)
    try:
        stale = serving.get_freshness(con)
        last_run = con.execute("SELECT month, status, error FROM pipeline_run ORDER BY run_id DESC LIMIT 1").fetchone()
    finally:
        con.close()
    assert stale["level"] == "stale"
    assert "2026-02" in stale["message"] and "failed" in stale["message"]
    assert stale["data_as_of"] == pd.Timestamp("2026-01-11 09:00")  # mart untouched
    assert last_run[:2] == ("2026-02", "failed") and "Raw file not found" in last_run[2]

    # Fix upstream and replay.
    write_month(env, "2026-02", make_month("2026-02"))
    run(env, "2026-02")
    con = read_only(env)
    try:
        recovered = serving.get_freshness(con)
    finally:
        con.close()
    assert recovered["level"] == "fresh"
    assert recovered["data_as_of"] == pd.Timestamp("2026-02-11 09:00")


def test_run_stuck_in_running_is_stale(env):
    write_month(env, "2026-01", make_month("2026-01"))
    run(env, "2026-01")
    con = database.get_connection(env["db_path"])
    try:
        started = pd.Timestamp("2026-10-03 06:00")
        database.start_pipeline_run(con, "2026-02", started)
        in_progress = serving.get_freshness(con, now=started + pd.Timedelta(minutes=10))
        stuck = serving.get_freshness(con, now=started + serving.STUCK_RUN_AFTER + pd.Timedelta(minutes=1))
    finally:
        con.close()
    assert in_progress["level"] == "fresh" and "in progress" in in_progress["message"]
    assert stuck["level"] == "stale" and "never finished" in stuck["message"]


def test_freshness_is_missing_before_first_publish(env):
    con = database.get_connection(env["db_path"])
    try:
        database.ensure_schema(con)
        assert serving.get_freshness(con)["level"] == "missing"
    finally:
        con.close()


def test_dashboard_shows_the_freshness_label(env, monkeypatch):
    monkeypatch.setenv("TAXI_DB_PATH", str(env["db_path"]))
    write_month(env, "2026-01", make_month("2026-01"))
    run(env, "2026-01")

    at = AppTest.from_file(APP_PATH, default_timeout=30).run()
    assert not at.exception
    assert at.success and "Data as of 2026-01-11 09:00" in at.success[0].value
    assert at.metric[0].value == "4"  # trips

    with pytest.raises(FileNotFoundError):
        run(env, "2026-02")
    at = AppTest.from_file(APP_PATH, default_timeout=30).run()
    assert not at.exception
    assert not at.success
    assert at.warning and "Stale data" in at.warning[0].value
