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

    # Ensure datetime columns are correctly parsed
    pickup_col = pd.to_datetime(df['tpep_pickup_datetime'])
    dropoff_col = pd.to_datetime(df['tpep_dropoff_datetime'])

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
