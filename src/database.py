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

    else:
        drop_fact_foreign_keys(con)

    for filename in ["05_create_quarantine.sql", "06_create_pipeline_run.sql"]:
        con.execute((SQL_DIR / filename).read_text(encoding="utf-8"))


def drop_fact_foreign_keys(con: duckdb.DuckDBPyConnection) -> bool:
    """
    One-off Phase 9 migration for a database built before the FOREIGN KEY constraints were
    removed from sql/03. DuckDB cannot drop a constraint in place, so fact_trip is rebuilt
    from the current DDL and every row (trip_key included) is copied across in a single
    transaction. Returns True if it migrated, False if there was nothing to do.
    """
    has_fks = con.execute("""
        SELECT COUNT(*) FROM duckdb_constraints()
        WHERE table_name = 'fact_trip' AND constraint_type = 'FOREIGN KEY'
    """).fetchone()[0]
    if not has_fks:
        return False

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("ALTER TABLE fact_trip RENAME TO fact_trip_phase8")
        con.execute((SQL_DIR / "03_create_fact.sql").read_text(encoding="utf-8"))
        con.execute("INSERT INTO fact_trip SELECT * FROM fact_trip_phase8")
        con.execute("DROP TABLE fact_trip_phase8")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return True


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


# fact_trip column -> (dimension table, dimension key). Declared FOREIGN KEY constraints
# until Phase 9; now enforced per batch by check_fact_foreign_keys.
FACT_FOREIGN_KEYS = {
    "date_key": ("dim_date", "date_key"),
    "pickup_location_key": ("dim_location", "location_key"),
    "dropoff_location_key": ("dim_location", "location_key"),
    "vendor_key": ("dim_vendor", "vendor_key"),
    "payment_type_key": ("dim_payment", "payment_type_key"),
    "rate_code_key": ("dim_rate_code", "rate_code_key"),
}


class ReferentialIntegrityError(ValueError):
    """A fact batch carries a dimension key with no matching dimension row."""


def check_fact_foreign_keys(con: duckdb.DuckDBPyConnection, batch_view: str) -> None:
    """
    Set-based replacement for the dropped FOREIGN KEY constraints: one anti-join per key
    over the whole batch, instead of six index lookups per row. A NULL key is allowed, as it
    was under the constraint (rate_code_key is NULL when no rate code was reported).
    Raises before anything is written, so a bad batch is refused whole.
    """
    orphans = {}
    for column, (dim_table, dim_key) in FACT_FOREIGN_KEYS.items():
        n = con.execute(f"""
            SELECT COUNT(*) FROM {batch_view} b
            WHERE b.{column} IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM {dim_table} d WHERE d.{dim_key} = b.{column})
        """).fetchone()[0]
        if n:
            orphans[column] = n
    if orphans:
        raise ReferentialIntegrityError(
            f"Fact batch refused: keys with no matching dimension row: {orphans}"
        )


def insert_fact_trips(con: duckdb.DuckDBPyConnection, fact_rows: pd.DataFrame) -> int:
    """
    Inserts fact rows, skipping any trip_id already present. Returns rows actually inserted.
    Refuses the whole batch (ReferentialIntegrityError) if any dimension key is unknown.
    """
    before = con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]

    con.register("fact_trip_batch", fact_rows)
    try:
        check_fact_foreign_keys(con, "fact_trip_batch")
        columns = ", ".join(fact_rows.columns)
        con.execute(f"""
            INSERT INTO fact_trip ({columns})
            SELECT {columns} FROM fact_trip_batch
            ON CONFLICT (trip_id) DO NOTHING
        """)
    finally:
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
