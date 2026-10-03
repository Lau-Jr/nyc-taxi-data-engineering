# Phase 8 — Leakage audit

**The question asked of every column:** *would this exact value have been knowable at
00:00 on the forecast day, in this system, with this data delay?* (See
[split_strategy.md](split_strategy.md) for the task and the prediction moment.)

Audited 2026-10-03, before any modelling. Every column that fails is cut, however well it
scores. The correlation figures below were measured on the **train split only**, so that
validation and test stay untouched. They show how tempting each cut column was.

## Columns in `features_zone_hour`

| Column | Role | Story | Knowable at 00:00? | Verdict |
|---|---|---|---|---|
| `zone_id` | key | TLC pickup zone | yes, fixed | keep |
| `hour_start` | key | the hour being forecast | yes, it is the question | keep |
| `forecast_date`, `prediction_made_at` | key | the day being forecast, and its midnight | yes | keep |
| `hour_of_day` | feature | Demand follows the clock: the morning and evening peaks | yes, calendar | keep |
| `day_of_week`, `is_weekend` | feature | Weekday commuting and weekend nightlife have different shapes | yes, calendar | keep |
| `is_public_holiday` | feature | MLK Day and Presidents' Day break the weekday pattern | yes, published a year ahead (`config/features.toml`) | keep |
| `trips_same_hour_lag_1d` | feature | Same zone, same hour, yesterday: the freshest full day | yes, ends ≥ 1 h before midnight | keep (r = 0.907) |
| `trips_same_hour_lag_7d` | feature | Same zone, same hour, one week ago: the weekly rhythm | yes | keep (r = 0.946) |
| `trips_same_hour_mean_7d` | feature | Mean of the last 7 days at this hour: smooths one-off days | yes, days D−7 … D−1 | keep (r = 0.915) |
| `zone_trips_prev_day` | feature | The zone's total yesterday: its current activity level | yes | keep (r = 0.755) |
| `zone_trips_trend_7d` | feature | Last 7 days ÷ the 7 before (+1 smoothed): is the zone heating up or cooling down? | yes, days D−14 … D−1 | keep |
| `history_complete` | flag | FALSE where the 7-day features lack 14 days of history (the slide 13 "was_missing" rule) | yes, depends only on the calendar | keep |
| `target_trips` | **target** | Pickups in this zone during this hour | **no, by definition**: it is the answer | target only, never a feature |
| `split` | bookkeeping | warmup / train / validate / test, from `config/features.toml` | n/a, not a feature | never fed to a model |

## Candidates considered and cut

| Candidate | Form of leakage | Why it fails the question | How tempting (train) |
|---|---|---|---|
| Revenue in the same zone-hour (`SUM(total_amount)`) | target | It is produced by the very trips being counted, so it is a disguised copy of the answer | r = **0.903** with the target |
| Dropoffs in the same zone-hour | future | Trips ending in that hour mostly start in it too, and none of it exists at 00:00 | r = **0.881** |
| The zone's total for the forecast day | future | It includes all 24 hours being forecast; only known at the following midnight | r = 0.795 |
| Average fare / anomaly share of the same zone-hour | target | Computed from the trips being predicted; also undefined (null) exactly when the target is 0, which tells the model the answer | not measured: fails on its face |
| A zone's average demand over the whole Jan–Feb table | preprocessing | It averages in validation and test days, so test-set statistics leak into training (slide 13). Instead, `src/baseline.py`'s `train_profile` computes it from **train rows only** | — |
| Weather on the forecast day (e.g. the 25–26 Jan dip) | future (if observed) | Observed weather for day D is only known after D. A *forecast* issued before midnight would be honest, but this repo has no weather source | — |
| Anything from `quarantine_trip` or `mart_daily_zone_trips.refreshed_at` | future / irrelevant | Written by the pipeline after the fact; describes processing, not demand | — |

## Executable check: truncation

Answering the question once, by hand, is not enough. Code changes later. So
`src/features.py::check_no_future_leakage` runs **on every build**:

1. For each day D in `config/features.toml` (`leakage_check_days`: 20 Jan in train, 10 Feb
   in validate, 20 Feb in test), rebuild the whole table from **only** the trips picked up
   before 00:00 on D.
2. Require every feature of day D to be **identical** to the full build. The target is
   excluded, because it is meant to be unknown.

If any feature reads data from D or later, its value changes and the build fails with
`LeakageError`. `tests/test_features.py::test_leakage_check_catches_a_leaky_feature`
proves that the check works: it swaps "same hour yesterday" for "same hour today" and
asserts the build is refused. On the real January–February data the check passes for all
three days (see the snapshot `manifest.json`, `leakage_check_days`).

## The data-delay caveat (slide 8's "in that system")

The lag features assume the operator has **yesterday's trips at midnight**, for example
from its own dispatch system. **TLC's public files do not allow that:** each month is
published as one file about two months later. Under that delay, "same hour yesterday" is
not knowable at prediction time. An honest TLC-only model would need lags of at least
about 60 days, which with two months of data leaves nothing to train on. So this table is
honest *for an operator with a live feed*, and the assumption is stated here and in
`DATASHEET.md` instead of left implicit.

## Contrast: the trip-level task that would leak badly

If the task were instead "predict a trip's fare when it is booked", most of `fact_trip`
would fail the question: `trip_distance`, `dropoff_datetime` (hence duration),
`tolls_amount`, `tip_amount`, `total_amount` and `is_anomaly` are only recorded **after**
the trip ends. A model using them would score superbly in testing and could not run on a
single new booking. Only the pickup time, pickup zone, vendor, and (if the rider enters
it) the destination zone would survive.
