# Phase 8 baselines: validate split

Snapshot `v2026-10-03` (sha256 `fc6a6e6b9c35...`), target `target_trips` = pickups per zone per hour, seed 42 (no randomness is used).
Regenerate with `python -m src.baseline --snapshot data/features/v2026-10-03`.

| Baseline | Rows scored | MAE (trips/hour) | RMSE |
|---|---:|---:|---:|
| `same_hour_last_week` | 62,880 | 5.047 | 15.028 |
| `train_profile` | 62,880 | 5.066 | 15.649 |
| `same_hour_mean_7d` | 62,880 | 6.613 | 20.825 |
| `same_hour_yesterday` | 62,880 | 6.659 | 22.180 |
