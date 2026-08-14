import logging
import pandas as pd

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


# Hard-reject checks: the record cannot be analytically valid, so it is excluded from the
# load and written to the rejected-rows log with a reason (docs/phase1 Problem B/C).
HARD_REJECT_CHECKS = {
    "missing_pickup_datetime": lambda df: df['tpep_pickup_datetime'].isna(),
    "missing_dropoff_datetime": lambda df: df['tpep_dropoff_datetime'].isna(),
    "missing_pickup_location": lambda df: df['PULocationID'].isna(),
    "missing_dropoff_location": lambda df: df['DOLocationID'].isna(),
    "dropoff_before_or_equal_pickup": lambda df: df['tpep_dropoff_datetime'] <= df['tpep_pickup_datetime'],
}

# Soft-anomaly checks: the record is kept and loaded, but flagged via fact_trip.is_anomaly
# so downstream queries can include/exclude it explicitly instead of it being silently
# dropped or silently biasing aggregates.
SOFT_ANOMALY_CHECKS = {
    "zero_or_missing_passenger_count": lambda df: df['passenger_count'].fillna(0) <= 0,
    "zero_or_negative_trip_distance": lambda df: df['trip_distance'] <= 0,
    "negative_fare_amount": lambda df: df['fare_amount'] < 0,
    "negative_total_amount": lambda df: df['total_amount'] < 0,
}


def get_hard_reject_reasons(df: pd.DataFrame) -> pd.Series:
    """
    Returns a Series aligned to df.index: a comma-separated list of hard-reject reason
    codes for rows that fail at least one hard check, and '' for rows that pass all of them.
    """
    reasons = pd.Series('', index=df.index, dtype=object)
    for reason, check in HARD_REJECT_CHECKS.items():
        mask = check(df)
        reasons = reasons.where(~mask, reasons + ',' + reason)
    return reasons.str.strip(',')


def get_soft_anomaly_mask(df: pd.DataFrame) -> pd.Series:
    """Returns a boolean Series: True if the row trips at least one soft-anomaly check."""
    mask = pd.Series(False, index=df.index)
    for check in SOFT_ANOMALY_CHECKS.values():
        mask = mask | check(df)
    return mask
