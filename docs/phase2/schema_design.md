# Schema Design

## Choice: star schema

A star schema was selected because the primary workload here is analytical aggregation
(revenue and demand by time, location, vendor, payment method), not transactional
record-keeping. Trip measurements — distance, passenger count, fare components — belong in
one fact table; the attributes used to slice and filter those measurements — date, location,
vendor, payment method, rate type — are pulled out into dimensions so they're stored once
each instead of repeated on every one of 3.7M trip rows.

```
                  dim_date
                     |
dim_location -- fact_trip -- dim_payment
                     |
                 dim_vendor
                     |
                 dim_rate_code
```

`fact_trip` holds one row per trip: the numeric measurements, plus a foreign key into each
dimension. See `docs/phase2/data_dictionary.md` for the full column list and
`sql/01_create_schema.sql`–`sql/03_create_fact.sql` for the DDL.

## Why not one big table?

The raw file already repeats a handful of low-cardinality codes — `VendorID`, `payment_type`,
`RatecodeID`, `PULocationID`/`DOLocationID` — across millions of rows. Denormalizing those
into full descriptive text on every fact row would:

- multiply storage for text that's identical across huge blocks of rows (e.g. every one of
  the 1,706,078 credit-card trips from Curb Mobility would carry the string
  `"Curb Mobility, LLC"` and `"Credit card"` individually instead of a 4-byte key each), and
- turn a one-row fix (e.g. correcting a vendor name) into a rewrite of every trip that vendor
  ever handled.

The fact table stores only the measurements and the keys; descriptive text lives once per
dimension row and is joined in on query. `sql/04_analytical_queries.sql` has working examples
of two- and three-way joins that demonstrate this.

## Deliberate mix, not pure normalization

The schema isn't a fully normalized snowflake — it's a star with two different key strategies
chosen per dimension, based on how each dimension's data becomes known:

- **`dim_vendor`, `dim_payment`, `dim_rate_code`** are small, closed, mostly-static code sets.
  They're seeded up front in `sql/02_create_dimensions.sql` with hand-assigned surrogate
  keys, *including an explicit "Unknown / not provided" member* for codes that show up in the
  raw data but aren't part of TLC's published dictionary (`payment_type = 0`, `RatecodeID =
  99` — Problem C in `docs/phase1/data_problem_statement.md`). Seeding those "unknown" rows up
  front means a fact row carrying an unrecognized code still satisfies the foreign key and
  still joins cleanly, instead of failing the FK constraint or silently vanishing from every
  aggregate query that joins the dimension.

- **`dim_date` and `dim_location`** are populated at ingest time instead
  (`src/transform.py::build_dim_date_rows` / `build_dim_location_rows`), because their
  contents depend on what's actually in the file being loaded (which dates, which zones) and
  there's no fixed dictionary to seed from — no TLC Taxi Zone Lookup CSV ships with this
  dataset drop, so `dim_location` only has the raw zone id, not borough/zone names. For both
  of these, the dimension key is set equal to the natural id (`date_key = YYYYMMDD`,
  `location_key = location_id`) rather than an arbitrary surrogate, since the natural id is
  already a small, stable, non-reused integer — a synthetic surrogate would add an extra
  lookup step for no benefit.

## Referential integrity is enforced by the database, not assumed by the loader

Every foreign key in `fact_trip` (`sql/03_create_fact.sql`) is declared with a `FOREIGN KEY
... REFERENCES` constraint, verified against DuckDB directly (attempting to insert a fact row
against a nonexistent dimension key raises a `Constraint Error`). `rate_code_key` is the one
nullable FK — a NULL there means "the source row never reported a rate code" (missingness),
which is a distinct fact from "the source row explicitly reported an unrecognized code"
(mapped to the seeded "Unknown" row). Collapsing those two into the same bucket would have
hidden the systematic-missingness finding from Phase 1.

## Idempotency key lives on the fact table, not derived from dimension state

`fact_trip.trip_id` is a deterministic hash of raw trip fields (see
`docs/phase3/idempotency_proof.md`), independent of the star schema's surrogate keys. This
is what lets `src/ingest.py` re-run the same month without needing to know what was
previously loaded — the `UNIQUE (trip_id)` constraint plus `ON CONFLICT DO NOTHING` does the
deduplication at the database level.
