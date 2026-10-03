"""
Unit 9: the fix (dropping the per-row FOREIGN KEY constraints on fact_trip) must not
weaken the guarantees. A batch with an unknown dimension key is still refused whole,
existing databases are migrated without losing a row, and every run reports its step
timings.
"""
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src import database, ingest, transform

from test_serving import make_month


def fresh_schema() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    database.ensure_schema(con)
    con.execute("INSERT INTO dim_date VALUES (20260110, '2026-01-10', 10, 1, 1, 2026, 6, 'Saturday')")
    con.execute("INSERT INTO dim_location VALUES (132, 132, NULL, NULL, NULL), (236, 236, NULL, NULL, NULL)")
    return con


def fact_rows(**overrides) -> pd.DataFrame:
    row = {
        "trip_id": "a" * 64, "date_key": 20260110, "pickup_location_key": 132,
        "dropoff_location_key": 236, "vendor_key": 2, "payment_type_key": 2, "rate_code_key": None,
        "pickup_datetime": pd.Timestamp("2026-01-10 08:00"), "dropoff_datetime": pd.Timestamp("2026-01-10 08:20"),
        "total_amount": 15.5, "is_anomaly": False,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_fact_trip_has_no_foreign_key_constraints():
    con = fresh_schema()
    kinds = {r[0] for r in con.execute(
        "SELECT constraint_type FROM duckdb_constraints() WHERE table_name = 'fact_trip'").fetchall()}
    assert "FOREIGN KEY" not in kinds
    assert "UNIQUE" in kinds  # idempotency guarantee kept


def test_valid_batch_loads_and_null_rate_code_is_allowed():
    con = fresh_schema()
    assert database.insert_fact_trips(con, fact_rows()) == 1
    assert database.insert_fact_trips(con, fact_rows()) == 0  # replay: UNIQUE(trip_id) still works


@pytest.mark.parametrize("column, bad_key", [
    ("date_key", 20991231), ("pickup_location_key", 999), ("dropoff_location_key", 999),
    ("vendor_key", 99), ("payment_type_key", 99), ("rate_code_key", 99),
])
def test_batch_with_unknown_dimension_key_is_refused_whole(column, bad_key):
    con = fresh_schema()
    batch = pd.concat([fact_rows(), fact_rows(trip_id="b" * 64, **{column: bad_key})], ignore_index=True)
    with pytest.raises(database.ReferentialIntegrityError, match=column):
        database.insert_fact_trips(con, batch)
    assert database.fact_trip_count(con) == 0  # the good row was not written either


def test_existing_database_with_foreign_keys_is_migrated_without_losing_rows():
    con = duckdb.connect()
    for filename in ["01_create_schema.sql", "02_create_dimensions.sql"]:
        con.execute((database.SQL_DIR / filename).read_text(encoding="utf-8"))
    # The Phase 2-8 table: same columns, plus a declared FOREIGN KEY.
    old_ddl = (database.SQL_DIR / "03_create_fact.sql").read_text(encoding="utf-8").replace(
        "CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id)",
        "CONSTRAINT uq_fact_trip_trip_id UNIQUE (trip_id),\n"
        "    CONSTRAINT fk_fact_trip_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key)")
    con.execute(old_ddl)
    con.execute("INSERT INTO dim_date VALUES (20260110, '2026-01-10', 10, 1, 1, 2026, 6, 'Saturday')")
    con.execute("INSERT INTO dim_location VALUES (132, 132, NULL, NULL, NULL), (236, 236, NULL, NULL, NULL)")
    database.insert_fact_trips(con, fact_rows())
    before = con.execute("SELECT * FROM fact_trip").fetchall()

    database.ensure_schema(con)

    fks = con.execute("SELECT COUNT(*) FROM duckdb_constraints() "
                      "WHERE table_name = 'fact_trip' AND constraint_type = 'FOREIGN KEY'").fetchone()[0]
    assert fks == 0
    assert con.execute("SELECT * FROM fact_trip").fetchall() == before  # trip_key included
    assert database.drop_fact_foreign_keys(con) is False  # nothing left to do


def test_every_run_reports_step_timings(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    make_month("2026-01").to_parquet(raw_dir / "yellow_tripdata_2026-01.parquet")
    summary = ingest.run("2026-01", raw_dir, tmp_path / "taxi.duckdb", tmp_path / "logs")
    assert {"read_parquet", "add_trip_id", "hard_checks", "dims_upsert", "fact_insert",
            "mart_refresh"} <= set(summary["step_seconds"])
    assert all(seconds >= 0 for seconds in summary["step_seconds"].values())
