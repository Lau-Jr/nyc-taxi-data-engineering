import hashlib

import pandas as pd

# Fields that make up the deterministic business key. The raw TLC file has no native
# trip_id, so we derive one from a stable combination of fields (see design notes in
# docs/phase1 and sql/03_create_fact.sql). This is a project-defined key, not an
# official TLC identifier.
TRIP_ID_FIELDS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
]

# raw column -> fact_trip column
FACT_COLUMN_RENAME = {
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "Airport_fee": "airport_fee",
}


def add_trip_id(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a deterministic trip_id column: sha256("|".join(TRIP_ID_FIELDS))."""
    # .map(str) (not .astype(str)) so NaN/NaT stringify to 'nan'/'NaT' instead of
    # propagating as null and poisoning the whole concatenated key with a null.
    key_str = df[TRIP_ID_FIELDS[0]].map(str)
    for field in TRIP_ID_FIELDS[1:]:
        key_str = key_str + "|" + df[field].map(str)

    df = df.copy()
    df["trip_id"] = key_str.map(lambda s: hashlib.sha256(s.encode()).hexdigest())
    return df


def add_date_key(df: pd.DataFrame) -> pd.DataFrame:
    """Adds an integer date_key (YYYYMMDD) derived from the pickup date."""
    df = df.copy()
    df["date_key"] = (
        df["tpep_pickup_datetime"].dt.strftime("%Y%m%d").astype(int)
    )
    return df


def build_dim_date_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Builds one dim_date row per distinct pickup date present in df."""
    dates = pd.to_datetime(df["tpep_pickup_datetime"].dt.date.unique())
    return pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "day": dates.day,
        "month": dates.month,
        "quarter": dates.quarter,
        "year": dates.year,
        "day_of_week": dates.dayofweek,
        "day_name": dates.day_name(),
    })


def build_dim_location_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds one dim_location row per distinct PU/DO location id present in df.
    borough/zone/service_zone are left NULL: no TLC Taxi Zone Lookup CSV ships with
    this dataset drop (see sql/02_create_dimensions.sql).
    """
    location_ids = pd.unique(
        pd.concat([df["PULocationID"], df["DOLocationID"]], ignore_index=True)
    )
    location_ids.sort()
    return pd.DataFrame({
        "location_key": location_ids,
        "location_id": location_ids,
        "borough": None,
        "zone": None,
        "service_zone": None,
    })


def prepare_fact_rows(df: pd.DataFrame, vendor_map: dict, payment_map: dict, rate_map: dict) -> pd.DataFrame:
    """
    Transforms a validated raw batch into rows shaped for fact_trip: renames columns,
    maps natural codes (VendorID, payment_type, RatecodeID) to their dimension surrogate
    keys, and points location/date foreign keys at the natural ids used as dim_location /
    dim_date keys (see build_dim_location_rows / build_dim_date_rows).
    """
    df = df.rename(columns=FACT_COLUMN_RENAME).copy()

    df["vendor_key"] = df["VendorID"].map(vendor_map)
    df["payment_type_key"] = df["payment_type"].map(payment_map)
    df["rate_code_key"] = df["RatecodeID"].map(rate_map)
    df["pickup_location_key"] = df["PULocationID"]
    df["dropoff_location_key"] = df["DOLocationID"]

    fact_columns = [
        "trip_id", "date_key", "pickup_location_key", "dropoff_location_key",
        "vendor_key", "payment_type_key", "rate_code_key",
        "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee",
        "cbd_congestion_fee", "total_amount", "is_anomaly",
    ]
    return df[fact_columns]
