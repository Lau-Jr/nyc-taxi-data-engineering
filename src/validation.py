import logging
import pandas as pd

from src import transform

# Configure logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_and_log_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks for data quality anomalies in NYC Yellow Taxi dataframe,
    logs the violation counts/percentages, and returns a summary report.
    """
    total_rows = len(df)
    if total_rows == 0:
        logging.warning("The provided DataFrame is empty.")
        return df

    logging.info(f"Starting data quality check on {total_rows:,} rows...")

    pickup_col = df['tpep_pickup_datetime']
    dropoff_col = df['tpep_dropoff_datetime']

    # Define the 5 quality checks
    checks = {
        "Passenger Count <= 0": df['passenger_count'] <= 0,
        "Trip Distance <= 0": df['trip_distance'] <= 0,
        "Fare Amount < 0": df['fare_amount'] < 0,
        "Total Amount < 0": df['total_amount'] < 0,
        "Dropoff Before Pickup": dropoff_col < pickup_col
    }

    # Loop through checks, calculate stats, and log them
    for check_name, condition in checks.items():
        violation_count = condition.sum()
        violation_pct = (violation_count / total_rows) * 100
        
        if violation_count > 0:
            logging.warning(
                f"{check_name}: found {violation_count:,} violations ({violation_pct:.2f}%)"
            )
        else:
            logging.info(f"{check_name}: 0 violations found.")

    # Calculate overall clean data metrics
    combined_anomaly_mask = pd.DataFrame(checks).any(axis=1)
    total_anomalies = combined_anomaly_mask.sum()
    
    logging.info(
        f"Summary: Found {total_anomalies:,} total invalid rows ({ (total_anomalies/total_rows)*100 :.2f}%). "
        f"{total_rows - total_anomalies:,} rows are completely clean."
    )

    return df


# Valid code sets, mirroring the seed rows in sql/02_create_dimensions.sql. ingest.run
# reloads them from the seeded dim tables at run time (build_check_context) so the dims
# stay the single source of truth; these defaults apply when no database is at hand.
DEFAULT_VALID_VENDOR_IDS = {1, 2, 6, 7}
DEFAULT_VALID_PAYMENT_TYPES = {0, 1, 2, 3, 4, 5, 6}
DEFAULT_VALID_RATE_CODES = {1, 2, 3, 4, 5, 6, 99}

# TLC taxi zones are numbered 1-263, plus 264 ("Unknown") and 265 ("Outside of NYC").
MIN_LOCATION_ID = 1
MAX_LOCATION_ID = 265

MAX_TRIP_DURATION = pd.Timedelta(hours=24)

# Batch-level completeness: a column whose null rate exceeds this is logged as a WARNING.
NULL_RATE_WARNING_THRESHOLD = 0.05


def build_check_context(month: str = None, valid_vendor_ids=None, valid_payment_types=None,
                        valid_rate_codes=None, now: pd.Timestamp = None) -> dict:
    """
    Bundles the run-level inputs some checks need: the --month being ingested, the valid
    code sets (loaded from the seeded dims by ingest.run), and "now" for the future-date check.
    """
    return {
        "month": month,
        "valid_vendor_ids": set(valid_vendor_ids or DEFAULT_VALID_VENDOR_IDS),
        "valid_payment_types": set(valid_payment_types or DEFAULT_VALID_PAYMENT_TYPES),
        "valid_rate_codes": set(valid_rate_codes or DEFAULT_VALID_RATE_CODES),
        "now": now if now is not None else pd.Timestamp.now(),
    }


def _pickup_outside_file_month(df: pd.DataFrame, ctx: dict) -> pd.Series:
    if not ctx.get("month"):
        return pd.Series(False, index=df.index)
    month_start = pd.Timestamp(f"{ctx['month']}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)
    pickup = df['tpep_pickup_datetime']
    return pickup.notna() & ((pickup < month_start) | (pickup >= month_end))


def _location_out_of_range(location_ids: pd.Series) -> pd.Series:
    return location_ids.notna() & ~location_ids.between(MIN_LOCATION_ID, MAX_LOCATION_ID)


def _duplicate_in_batch(df: pd.DataFrame, ctx: dict) -> pd.Series:
    # trip_id is the deterministic business key (src/transform.py). The first occurrence
    # is kept; every later copy in the same batch is quarantined.
    trip_ids = df['trip_id'] if 'trip_id' in df.columns else transform.add_trip_id(df)['trip_id']
    return trip_ids.duplicated(keep='first')


# Hard-reject checks: the record cannot be analytically valid, so it is excluded from the
# load and written to quarantine_trip (plus logs/rejected_<month>.csv) with a reason code
# (docs/phase1 Problem B/C). Each check takes (df, ctx) — see build_check_context.
HARD_REJECT_CHECKS = {
    "missing_pickup_datetime": lambda df, ctx: df['tpep_pickup_datetime'].isna(),
    "missing_dropoff_datetime": lambda df, ctx: df['tpep_dropoff_datetime'].isna(),
    "missing_pickup_location": lambda df, ctx: df['PULocationID'].isna(),
    "missing_dropoff_location": lambda df, ctx: df['DOLocationID'].isna(),
    "dropoff_before_or_equal_pickup": lambda df, ctx: df['tpep_dropoff_datetime'] <= df['tpep_pickup_datetime'],
    "pickup_in_future": lambda df, ctx: df['tpep_pickup_datetime'] > ctx['now'],
    "pickup_outside_file_month": _pickup_outside_file_month,
    "unknown_vendor": lambda df, ctx: ~df['VendorID'].isin(ctx['valid_vendor_ids']),
    "unknown_payment_type": lambda df, ctx: df['payment_type'].notna() & ~df['payment_type'].isin(ctx['valid_payment_types']),
    "unknown_rate_code": lambda df, ctx: df['RatecodeID'].notna() & ~df['RatecodeID'].isin(ctx['valid_rate_codes']),
    "location_id_out_of_range": lambda df, ctx: _location_out_of_range(df['PULocationID']) | _location_out_of_range(df['DOLocationID']),
    "duplicate_in_batch": _duplicate_in_batch,
}

# Soft-anomaly checks: the record is kept and loaded, but flagged via fact_trip.is_anomaly
# so downstream queries can include/exclude it explicitly instead of it being silently
# dropped or silently biasing aggregates.
SOFT_ANOMALY_CHECKS = {
    "zero_or_missing_passenger_count": lambda df: df['passenger_count'].fillna(0) <= 0,
    "zero_or_negative_trip_distance": lambda df: df['trip_distance'] <= 0,
    "negative_fare_amount": lambda df: df['fare_amount'] < 0,
    "negative_total_amount": lambda df: df['total_amount'] < 0,
    "trip_over_24h": lambda df: (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']) > MAX_TRIP_DURATION,
}

# Data-quality dimension each row-level check guards. Completeness is also checked at
# batch level by log_completeness.
CHECK_DIMENSIONS = {
    "missing_pickup_datetime": "completeness",
    "missing_dropoff_datetime": "completeness",
    "missing_pickup_location": "completeness",
    "missing_dropoff_location": "completeness",
    "dropoff_before_or_equal_pickup": "validity",
    "pickup_in_future": "validity",
    "pickup_outside_file_month": "validity",
    "unknown_vendor": "validity",
    "unknown_payment_type": "validity",
    "unknown_rate_code": "validity",
    "location_id_out_of_range": "validity",
    "duplicate_in_batch": "uniqueness",
    "zero_or_missing_passenger_count": "validity",
    "zero_or_negative_trip_distance": "validity",
    "negative_fare_amount": "validity",
    "negative_total_amount": "validity",
    "trip_over_24h": "validity",
}


def get_hard_reject_reasons(df: pd.DataFrame, ctx: dict = None) -> pd.Series:
    """
    Returns a Series aligned to df.index: a comma-separated list of hard-reject reason
    codes for rows that fail at least one hard check, and '' for rows that pass all of them.
    """
    ctx = ctx or build_check_context()
    reasons = pd.Series('', index=df.index, dtype=object)
    for reason, check in HARD_REJECT_CHECKS.items():
        mask = check(df, ctx).fillna(False).astype(bool)
        reasons = reasons.where(~mask, reasons + ',' + reason)
    return reasons.str.strip(',')


def count_reasons(reasons: pd.Series) -> dict:
    """Per-reason-code row counts from get_hard_reject_reasons output (a row can carry several)."""
    codes = reasons[reasons != ''].str.split(',').explode()
    return {code: int(n) for code, n in codes.value_counts().items()}


def get_soft_anomaly_mask(df: pd.DataFrame) -> pd.Series:
    """Returns a boolean Series: True if the row trips at least one soft-anomaly check."""
    mask = pd.Series(False, index=df.index)
    for check in SOFT_ANOMALY_CHECKS.values():
        mask = mask | check(df).fillna(False).astype(bool)
    return mask


def get_soft_anomaly_counts(df: pd.DataFrame) -> dict:
    """Per-check row counts for the soft-anomaly checks."""
    return {name: int(check(df).fillna(False).sum()) for name, check in SOFT_ANOMALY_CHECKS.items()}


def log_completeness(df: pd.DataFrame, threshold: float = NULL_RATE_WARNING_THRESHOLD) -> dict:
    """
    Batch-level completeness check: logs each column's null rate, as a WARNING above
    threshold, and returns {column: null_rate}. Nulls in optional columns are not rejected
    row by row (~29% of January rows have no passenger_count/RatecodeID), but a jump in
    these rates from one month to the next is exactly what this log makes visible.
    """
    if df.empty:
        return {}
    null_rates = df.isna().mean()
    for column, rate in null_rates.items():
        if rate > threshold:
            logging.warning(f"Completeness: {column} is {rate:.2%} null (threshold {threshold:.0%})")
        elif rate > 0:
            logging.info(f"Completeness: {column} is {rate:.2%} null")
    return {column: float(rate) for column, rate in null_rates.items()}
