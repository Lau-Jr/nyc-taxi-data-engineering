"""
Published metric formulas, version METRICS_VERSION. This is the only place they are computed:
every consumer (the dashboard, tests, any future report) calls summarise() on
mart_daily_zone_trips rather than writing its own formula. metrics.md documents each one
(formula, grain, filters, owner) and must be updated in the same commit as this file.

The mart stores only additive columns (counts and sums). Ratios are derived here, after
rolling up to the grain the consumer asks for, so a monthly average fare is
sum(fares) / sum(paid trips), not the average of 31 daily averages.
"""
import pandas as pd

METRICS_VERSION = "1.0"

ADDITIVE_COLUMNS = [
    "trip_count",
    "anomaly_trip_count",
    "total_revenue",
    "paid_trip_count",
    "paid_fare_sum",
    "distance_trip_count",
    "distance_sum",
    "card_fare_sum",
    "card_tip_sum",
]

METRIC_COLUMNS = [
    "trip_count",
    "total_revenue",
    "avg_fare",
    "avg_trip_distance",
    "card_tip_rate",
    "anomaly_share",
]


def _ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.where(denominator != 0)


def summarise(mart: pd.DataFrame, by: list = None) -> pd.DataFrame:
    """
    Rolls mart_daily_zone_trips rows up to the grain `by` (e.g. ["pickup_date"],
    ["pickup_location_id"], or None for one grand-total row) and returns the published
    metrics at that grain.
    """
    if by:
        sums = mart.groupby(by, dropna=False)[ADDITIVE_COLUMNS].sum().reset_index()
    else:
        sums = mart[ADDITIVE_COLUMNS].sum().to_frame().T
    sums[ADDITIVE_COLUMNS] = sums[ADDITIVE_COLUMNS].astype(float)

    out = sums[by].copy() if by else pd.DataFrame(index=sums.index)
    out["trip_count"] = sums["trip_count"].astype(int)
    out["total_revenue"] = sums["total_revenue"]
    out["avg_fare"] = _ratio(sums["paid_fare_sum"], sums["paid_trip_count"])
    out["avg_trip_distance"] = _ratio(sums["distance_sum"], sums["distance_trip_count"])
    out["card_tip_rate"] = _ratio(sums["card_tip_sum"], sums["card_fare_sum"])
    out["anomaly_share"] = _ratio(sums["anomaly_trip_count"], sums["trip_count"])
    return out
