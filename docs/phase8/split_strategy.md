# Phase 8 — Split strategy

*Written and fixed in `config/features.toml` on 2026-10-03, **before** any model or
baseline was scored on validation or test data. The only score computed before this was
written was a smoke test of `src/baseline.py` on the train split.*

## The prediction task

| | |
|---|---|
| Row (one example) | one pickup zone × one hour |
| Target | `target_trips`: pickups in that zone during that hour, including real zeros |
| Prediction moment | **00:00 on the forecast day.** An operator plans the next day's fleet at midnight, so every feature must be knowable at that moment (see [leakage_audit.md](leakage_audit.md)). |
| Data | Jan–Feb 2026 TLC yellow-taxi trips that passed the Unit 6 hard checks: 262 zones × 59 days × 24 hours = 370,992 rows |

## The split: by time, never at random

```
2026-01-01 ─ 01-14   WARMUP     88,032 rows   fewer than 14 days of history: 7-day features incomplete; not used
2026-01-15 ─ 02-07   TRAIN     150,912 rows   24 days: learn patterns
2026-02-08 ─ 02-17   VALIDATE   62,880 rows   10 days: tune, compare, choose (incl. Presidents' Day, 16 Feb)
2026-02-18 ─ 02-28   TEST       69,168 rows   11 days: touched ONCE, at the very end
```

The boundaries live in `config/features.toml` `[split]`. Every row carries its `split`
label in the feature table, and the snapshot manifest records the boundaries.

**Why by time.** Hourly demand is strongly autocorrelated: 08:00 and 09:00 on the same day
in the same zone are near-copies of each other. A random split would put one in train and
the other in test, so the model would be scored on neighbours of rows it has already seen.
That is split leakage, and it would give optimistic numbers that melt in production. The
model will always be used to predict days after the data it learned from, so it is judged
the same way: train on the past, validate on the recent, test on the newest.

**Why not group-aware (holding out zones).** The use case is forecasting the same 262 TLC
zones the fleet already serves, never a new zone. Every zone therefore appears in train,
validate and test *by design*. Holding out whole zones would answer a different question
("how well does this generalise to an unseen zone?") that no user is asking. If the task
changes to new areas, this decision must be revisited.

**Why a warm-up period.** The 7-day features need 14 days of history to be complete. Rows
before 15 January are kept in the table, with `history_complete = FALSE`, so the gap is
visible. They are not used for training or scoring.

## Rules for using the split

1. **Fit on train only.** Anything learned from data, such as scalers, imputation values or
   zone averages, is fitted on `split = 'train'` rows and applied unchanged to validate and
   test. The `train_profile` baseline in `src/baseline.py` follows this rule.
2. **Tune on validate.** Model choice and hyperparameters are compared on validation scores
   only.
3. **Touch test once.** `src/baseline.py` refuses `--split test` unless `--final` is passed.
   The test score is reported once, at the end, and never used to choose between
   alternatives.
4. **Never shuffle across splits.** Within train, any cross-validation must also go forward
   in time (for example, expanding windows), never k-fold shuffling.
5. **Changing a boundary spends the test set.** If `config/features.toml` `[split]` changes
   after test results have been seen, those results no longer count.

## Known limitations of this split

- **Only two months of data.** Train is 24 days, so the model sees each weekday only three
  or four times and no seasonal change at all. Adding more TLC months (download plus
  ingest) would allow a longer train window and a validation month.
- **January's weather dip.** The 25–26 January drop in trips (visible in the Phase 7
  dashboard) falls in train. That is good for robustness, but it means train contains one
  atypical weekend.
- **Validation contains a holiday (16 Feb).** There is one other holiday (19 Jan) in
  train, so the model sees `is_public_holiday = TRUE` once before validation.
- **Data delay.** The split assumes the operator has the previous day's trips at midnight
  (a live dispatch feed). Using only TLC's public monthly files, which arrive about 2
  months late, the lag features would not exist at prediction time. See
  [leakage_audit.md](leakage_audit.md).
