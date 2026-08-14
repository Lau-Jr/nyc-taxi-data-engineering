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
    """
    exists = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = 'fact_trip'"
    ).fetchone()
    if exists:
        return

    for filename in ["01_create_schema.sql", "02_create_dimensions.sql", "03_create_fact.sql"]:
        sql_text = (SQL_DIR / filename).read_text(encoding="utf-8")
        con.execute(sql_text)


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
