"""
Phase 7 consumer view: reads ONLY the published serving layer (mart_daily_zone_trips plus
pipeline_run for freshness), never fact_trip or raw files. Every number comes from
src/metrics.py, so it matches metrics.md and any other consumer.

    streamlit run streamlit_app.py

Set TAXI_DB_PATH to point at a different DuckDB file (the tests do).
"""
import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from src import database, metrics, serving

DB_PATH = Path(os.environ.get("TAXI_DB_PATH", database.DEFAULT_DB_PATH))

st.set_page_config(page_title="NYC taxi trips", page_icon=":material/local_taxi:", layout="wide")
st.title("NYC yellow taxi: trips and revenue by pickup zone")
st.caption(
    f"Reads `{serving.MART_TABLE}` only · metric definitions v{metrics.METRICS_VERSION}, "
    "see `metrics.md` · owner: Lau-Jr"
)


@st.cache_data(max_entries=4)
def load_mart(db_path: str, refreshed_at: str) -> pd.DataFrame:
    """Cached per mart version: a pipeline refresh changes refreshed_at and invalidates it."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        return serving.load_mart(con)
    finally:
        con.close()


# Freshness is recomputed on every rerun (it is two tiny queries), never cached.
if not DB_PATH.exists():
    st.error(f"No database at `{DB_PATH}`. Run `python -m src.ingest --month 2026-01`.",
             icon=":material/error:")
    st.stop()
try:
    con = duckdb.connect(str(DB_PATH), read_only=True)
except duckdb.IOException:
    st.warning("The pipeline is writing to the database right now, so the serving layer "
               "can't be read. Refresh this page when the ingest run finishes.",
               icon=":material/sync:")
    st.stop()
try:
    freshness = serving.get_freshness(con)
finally:
    con.close()

if freshness["level"] == "missing":
    st.error(freshness["message"], icon=":material/error:")
    st.stop()
elif freshness["level"] == "stale":
    st.warning(f"**Stale data.** {freshness['message']}", icon=":material/warning:")
else:
    st.success(freshness["message"], icon=":material/check_circle:")

mart = load_mart(str(DB_PATH), str(freshness["refreshed_at"]))
mart["pickup_date"] = pd.to_datetime(mart["pickup_date"])

# Drill path: whole city -> one pickup zone -> its daily detail table.
with st.sidebar:
    st.header("Filters")
    first_day, last_day = mart["pickup_date"].min().date(), mart["pickup_date"].max().date()
    date_range = st.date_input("Pickup dates", value=(first_day, last_day),
                               min_value=first_day, max_value=last_day)
    zones_by_trips = (metrics.summarise(mart.dropna(subset=["pickup_location_id"]), by=["pickup_location_id"])
                      .sort_values("trip_count", ascending=False)["pickup_location_id"]
                      .astype(int).tolist())
    zone = st.selectbox("Pickup zone", [None] + zones_by_trips,
                        format_func=lambda z: "All zones" if z is None else f"Zone {z}",
                        help="Zones are TLC location ids, ordered by trip count.")

start, end = (date_range[0], date_range[-1]) if date_range else (first_day, last_day)
view = mart[mart["pickup_date"].between(pd.Timestamp(start), pd.Timestamp(end))]
if zone is not None:
    view = view[view["pickup_location_id"] == zone]
if view.empty:
    st.info("No trips match these filters.", icon=":material/filter_alt_off:")
    st.stop()

total = metrics.summarise(view).iloc[0]
daily = metrics.summarise(view, by=["pickup_date"])

scope = "all zones" if zone is None else f"zone {zone}"
st.subheader(f"{start:%d %b %Y} to {end:%d %b %Y}, {scope}")

def money(value: float) -> str:
    return f"${value / 1e6:,.1f}M" if abs(value) >= 1e6 else f"${value:,.0f}"


def daily_trend(column: str) -> list:
    return daily[column].fillna(0).tolist()


# Two rows of three, each KPI with its daily trend for the selected dates.
with st.container(horizontal=True):
    st.metric("Trips", f"{int(total['trip_count']):,}", border=True,
              chart_data=daily_trend("trip_count"), chart_type="bar")
    st.metric("Total revenue", money(total["total_revenue"]), border=True,
              chart_data=daily_trend("total_revenue"), chart_type="bar",
              help=f"${total['total_revenue']:,.2f}. Everything charged, net of refunds.")
    st.metric("Average fare", f"${total['avg_fare']:,.2f}", border=True,
              chart_data=daily_trend("avg_fare"),
              help="Metered fare only, over trips with a positive fare.")
with st.container(horizontal=True):
    st.metric("Average distance", f"{total['avg_trip_distance']:,.2f} mi", border=True,
              chart_data=daily_trend("avg_trip_distance"),
              help="Over trips between 0 and 100 miles; longer ones are odometer errors.")
    st.metric("Card tip rate", f"{total['card_tip_rate']:.1%}", border=True,
              chart_data=daily_trend("card_tip_rate"),
              help="Tips ÷ fares on credit-card trips. TLC only records card tips.")
    st.metric("Anomaly share", f"{total['anomaly_share']:.1%}", border=True,
              chart_data=daily_trend("anomaly_share"),
              help="Trips flagged by a soft quality check. They are included in every "
                   "number above; see metrics.md.")

left, right = st.columns(2)
with left:
    with st.container(border=True):
        st.markdown("**How does daily demand move?**")
        st.line_chart(daily, x="pickup_date", y="trip_count", x_label="Pickup date",
                      y_label="Trips", alt="Trips per pickup day")
with right:
    with st.container(border=True):
        if zone is None:
            st.markdown("**Which pickup zones earn the most?**")
            top = (metrics.summarise(view.dropna(subset=["pickup_location_id"]), by=["pickup_location_id"])
                   .nlargest(10, "total_revenue"))
            top["zone"] = "Zone " + top["pickup_location_id"].astype(int).astype(str)
            st.bar_chart(top, x="zone", y="total_revenue", horizontal=True,
                         sort="-total_revenue", x_label="", y_label="Total revenue ($)",
                         alt="Top 10 pickup zones by total revenue")
        else:
            st.markdown(f"**How much does zone {zone} earn per day?**")
            st.bar_chart(daily, x="pickup_date", y="total_revenue", x_label="Pickup date",
                         y_label="Total revenue ($)", alt=f"Revenue per day in zone {zone}")

with st.expander("Daily detail"):
    st.dataframe(
        daily.sort_values("pickup_date", ascending=False),
        hide_index=True,
        column_config={
            "pickup_date": st.column_config.DateColumn("Pickup date"),
            "trip_count": st.column_config.NumberColumn("Trips", format="localized"),
            "total_revenue": st.column_config.NumberColumn("Total revenue", format="dollar"),
            "avg_fare": st.column_config.NumberColumn("Avg fare", format="dollar"),
            "avg_trip_distance": st.column_config.NumberColumn("Avg distance (mi)", format="%.2f"),
            "card_tip_rate": st.column_config.NumberColumn("Card tip rate", format="percent"),
            "anomaly_share": st.column_config.NumberColumn("Anomaly share", format="percent"),
        },
        alt="Daily metrics for the selected zone and dates",
    )
