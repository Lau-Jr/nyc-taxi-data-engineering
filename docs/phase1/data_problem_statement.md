# Data Problem Statement

**Project:** Design and Implementation of a Data Engineering Pipeline for NYC Taxi Trip Analytics
**Dataset:** NYC TLC Yellow Taxi Trip Records, January 2026 (`yellow_tripdata_2026-01.parquet`, 3,724,889 rows, 20 columns)

## Who needs this data?

Taxi transportation analysts and operational decision-makers at TLC-affiliated vendors and city
transportation planners.

## What decision does it support?

Understanding where and when taxi demand and revenue are concentrated, so that vehicle
supply, pricing, and congestion policy can be targeted by location and time of day.

## What is broken? (measured on the January 2026 file)

**Problem A — Systematic missingness in one reporting stream.**
Five columns — `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`,
`Airport_fee` — are null on exactly **1,088,058 rows (29.21%)**. This is not random: every one of
those rows also has `payment_type = 0`, and the two masks are identical. One upstream reporting
path (vendor terminals that don't send a payment-settlement record) simply never populates these
fields. A naive `dropna()` would delete almost a third of January's trips and bias every
downstream revenue and occupancy metric.

**Problem B — Implausible trip measurements.**
| Check | Rows | % of total |
|---|---:|---:|
| `trip_distance <= 0` | 125,738 | 3.38% |
| `dropoff_datetime <= pickup_datetime` | 45,070 | 1.21% |
| `total_amount < 0` | 39,984 | 1.07% |
| `fare_amount < 0` | 39,463 | 1.06% |
| `passenger_count == 0` | 14,787 | 0.40% |

`total_amount` ranges from -2,560.20 to +2,560.20 — a mirrored range, consistent with refund /
chargeback reversal records rather than random noise. These need to be segregated, not blindly
deleted.

**Problem C — Broken reference-code domains.**
The dataset uses foreign-key-style codes that don't match TLC's published dictionary:
`RatecodeID = 99` appears on 110,864 rows (TLC defines only 1–6); `payment_type = 0` appears on
1,088,058 rows (TLC defines only 1–6); `PULocationID` is 264 ("N/A") or 265 ("Unknown") on 5,930
rows. Any star-schema join against clean dimension tables will silently drop these rows unless
the dimensions explicitly model "Unknown" members.

By contrast, exact-row duplicates and duplicates on a deterministic 9-column business key
(`VendorID`, pickup/dropoff timestamp, PU/DO location, passenger count, distance, fare, total)
are both **zero** in this file — duplication is not a live problem here, but the absence of a
native `trip_id` is still a modelling risk worth addressing in Phase 3.
