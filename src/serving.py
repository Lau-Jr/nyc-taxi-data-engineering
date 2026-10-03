"""
Read side of the serving layer: loading the published mart and computing its freshness.

Freshness promise: mart_daily_zone_trips is rebuilt by the pipeline at the end of every
successful ingest run, so it always reflects every month that has been ingested
successfully. The label is computed from the data (MAX(last_pickup_datetime) and
refreshed_at in the mart) and from pipeline_run, never typed by hand. It turns stale when
the promise is broken:
  - the latest ingest run failed, so the mart is missing whatever that run was loading;
  - the latest run has been 'running' for longer than STUCK_RUN_AFTER (it crashed without
    recording a result);
  - the mart was never published, or no pipeline run is recorded for it.
"""
import duckdb
import pandas as pd

MART_TABLE = "mart_daily_zone_trips"

# A full month currently ingests in about 15 minutes; a run still 'running' after this long
# is assumed to have crashed.
STUCK_RUN_AFTER = pd.Timedelta(hours=2)


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone() is not None


def load_mart(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute(f"SELECT * FROM {MART_TABLE} ORDER BY pickup_date, pickup_location_id").fetchdf()


def get_freshness(con: duckdb.DuckDBPyConnection, now: pd.Timestamp = None) -> dict:
    """
    Returns {"level", "message", "data_as_of", "refreshed_at", "latest_run"}.
    level is one of "fresh", "stale" or "missing".
    """
    now = now if now is not None else pd.Timestamp.now()
    result = {"level": "missing", "message": "", "data_as_of": None, "refreshed_at": None,
              "latest_run": None}

    if not _table_exists(con, MART_TABLE):
        result["message"] = f"{MART_TABLE} has not been published yet. Run the ingest pipeline."
        return result

    data_as_of, refreshed_at = con.execute(
        f"SELECT MAX(last_pickup_datetime), MAX(refreshed_at) FROM {MART_TABLE}"
    ).fetchone()
    result["data_as_of"] = pd.Timestamp(data_as_of) if data_as_of is not None else None
    result["refreshed_at"] = pd.Timestamp(refreshed_at) if refreshed_at is not None else None

    latest = None
    if _table_exists(con, "pipeline_run"):
        latest = con.execute(
            "SELECT run_id, month, started_at, finished_at, status, error "
            "FROM pipeline_run ORDER BY run_id DESC LIMIT 1"
        ).fetchdf()
        latest = latest.iloc[0].to_dict() if not latest.empty else None
    result["latest_run"] = latest

    as_of = _fmt(result["data_as_of"])
    if latest is None:
        result["level"] = "stale"
        result["message"] = (f"No pipeline run is recorded for this mart, so its freshness cannot "
                             f"be vouched for. Showing data as of {as_of}.")
    elif latest["status"] == "failed":
        result["level"] = "stale"
        result["message"] = (f"The latest refresh failed: ingest run {latest['run_id']} for "
                             f"{latest['month']} at {_fmt(latest['finished_at'])} "
                             f"({latest['error']}). Showing data as of {as_of}, last refreshed "
                             f"{_fmt(result['refreshed_at'])}.")
    elif latest["status"] == "running" and now - pd.Timestamp(latest["started_at"]) > STUCK_RUN_AFTER:
        result["level"] = "stale"
        result["message"] = (f"Ingest run {latest['run_id']} for {latest['month']} started "
                             f"{_fmt(latest['started_at'])} and never finished. Showing data "
                             f"as of {as_of}.")
    else:
        result["level"] = "fresh"
        result["message"] = (f"Data as of {as_of}. Refreshed by the pipeline at "
                             f"{_fmt(result['refreshed_at'])}.")
        if latest["status"] == "running":
            result["message"] += f" A refresh for {latest['month']} is in progress."
    return result


def _fmt(ts) -> str:
    if ts is None or pd.isna(ts):
        return "unknown"
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M")
