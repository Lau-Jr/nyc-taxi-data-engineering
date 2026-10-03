from pathlib import Path

import duckdb
import pandas as pd

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "taxi.duckdb"


def get_connection(db_path: Path = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Creates the star schema on first run only. sql/01_create_schema.sql drops and
    recreates every table, so it must NOT be re-run on every ingest call — doing so
    would wipe previously loaded data and break idempotency between runs.

    sql/05_create_quarantine.sql and sql/06_create_pipeline_run.sql are CREATE ... IF NOT
    EXISTS and run every time, so a database built before those tables existed gains them
    without a rebuild.
    """
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'fact_trip'"
    ).fetchone()
    if not exists:
        for filename in ["01_create_schema.sql", "02_create_dimensions.sql", "03_create_fact.sql"]:
            sql_text = (SQL_DIR / filename).read_text(encoding="utf-8")
            con.execute(sql_text)

    for filename in ["05_create_quarantine.sql", "06_create_pipeline_run.sql"]:
        con.execute((SQL_DIR / filename).read_text(encoding="utf-8"))


def load_code_map(con: duckdb.DuckDBPyConnection, table: str, id_col: str, key_col: str) -> dict:
    rows = con.execute(f"SELECT {id_col}, {key_col} FROM {table}").fetchall()
    return dict(rows)


def upsert_dim_date(con: duckdb.DuckDBPyConnection, dim_date_rows: pd.DataFrame) -> None:
    con.register("dim_date_batch", dim_date_rows)
    con.execute("""
        INSERT INTO dim_date
        SELECT * FROM dim_date_batch
        ON CONFLICT (date_key) DO NOTHING
    """)
    con.unregister("dim_date_batch")


def upsert_dim_location(con: duckdb.DuckDBPyConnection, dim_location_rows: pd.DataFrame) -> None:
    con.register("dim_location_batch", dim_location_rows)
    con.execute("""
        INSERT INTO dim_location
        SELECT * FROM dim_location_batch
        ON CONFLICT (location_key) DO NOTHING
    """)
    con.unregister("dim_location_batch")


def insert_fact_trips(con: duckdb.DuckDBPyConnection, fact_rows: pd.DataFrame) -> int:
    """Inserts fact rows, skipping any trip_id already present. Returns rows actually inserted."""
    before = con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]

    con.register("fact_trip_batch", fact_rows)
    columns = ", ".join(fact_rows.columns)
    con.execute(f"""
        INSERT INTO fact_trip ({columns})
        SELECT {columns} FROM fact_trip_batch
        ON CONFLICT (trip_id) DO NOTHING
    """)
    con.unregister("fact_trip_batch")

    after = con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]
    return after - before


def fact_trip_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]


def insert_quarantine_rows(con: duckdb.DuckDBPyConnection, quarantine_rows: pd.DataFrame) -> int:
    """
    Inserts hard-rejected rows into quarantine_trip, skipping any trip_id already
    quarantined (so replaying a month is idempotent). Returns rows actually inserted.
    """
    if quarantine_rows.empty:
        return 0
    before = con.execute("SELECT COUNT(*) FROM quarantine_trip").fetchone()[0]

    con.register("quarantine_batch", quarantine_rows)
    columns = ", ".join(quarantine_rows.columns)
    con.execute(f"""
        INSERT INTO quarantine_trip ({columns})
        SELECT {columns} FROM quarantine_batch
        ON CONFLICT (trip_id) DO NOTHING
    """)
    con.unregister("quarantine_batch")

    after = con.execute("SELECT COUNT(*) FROM quarantine_trip").fetchone()[0]
    return after - before


def quarantine_trip_count(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute("SELECT COUNT(*) FROM quarantine_trip").fetchone()[0]


def start_pipeline_run(con: duckdb.DuckDBPyConnection, month: str, started_at: pd.Timestamp) -> int:
    """Records a new ingest run as 'running' and returns its run_id."""
    return con.execute(
        "INSERT INTO pipeline_run (month, started_at, status) VALUES (?, ?, 'running') RETURNING run_id",
        [month, started_at.to_pydatetime()],
    ).fetchone()[0]


def finish_pipeline_run(con: duckdb.DuckDBPyConnection, run_id: int, status: str,
                        finished_at: pd.Timestamp, error: str = None,
                        rows_inserted: int = None, mart_rows: int = None) -> None:
    con.execute(
        """
        UPDATE pipeline_run
        SET status = ?, finished_at = ?, error = ?, rows_inserted = ?, mart_rows = ?
        WHERE run_id = ?
        """,
        [status, finished_at.to_pydatetime(), error, rows_inserted, mart_rows, run_id],
    )


def refresh_marts(con: duckdb.DuckDBPyConnection, refreshed_at: pd.Timestamp) -> int:
    """
    Rebuilds mart_daily_zone_trips from fact_trip (sql/07) and returns its row count.
    A full rebuild is idempotent and takes about a second at this data size.
    """
    sql_text = (SQL_DIR / "07_refresh_mart_daily_zone_trips.sql").read_text(encoding="utf-8")
    con.execute(sql_text, {"refreshed_at": refreshed_at.to_pydatetime()})
    return con.execute("SELECT COUNT(*) FROM mart_daily_zone_trips").fetchone()[0]
