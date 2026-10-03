"""
Phase 8: naive baselines scored against a feature snapshot, to show the split works and
to set the bar any real model has to beat. Not a model.

Usage:
    python -m src.baseline                         # latest snapshot, validation split
    python -m src.baseline --snapshot data/features/v2026-10-03

Reads the immutable snapshot, never the live database. Scores the VALIDATION split only.
The test split is refused unless --final is passed. That flag is for the single, final
evaluation described in docs/phase8/split_strategy.md.
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src import features

PROJECT_ROOT = features.PROJECT_ROOT
RESULTS_DIR = PROJECT_ROOT / "results"


def latest_snapshot(root: Path = features.SNAPSHOT_ROOT) -> Path:
    versions = sorted(p for p in root.glob("v*") if (p / "manifest.json").exists())
    if not versions:
        raise FileNotFoundError(f"No feature snapshot under {root}. Run: python -m src.features")
    return versions[-1]


def load_snapshot(snapshot_dir: Path) -> tuple:
    manifest = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    table = pd.read_parquet(snapshot_dir / manifest["file"])
    if features._sha256(snapshot_dir / manifest["file"]) != manifest["sha256"]:
        raise ValueError(f"{snapshot_dir} does not match its manifest sha256: it was modified")
    return table, manifest


def predict(train: pd.DataFrame, scored: pd.DataFrame) -> pd.DataFrame:
    """
    One column per baseline. The only one fitted on data is train_profile, and it is
    fitted on the TRAIN rows alone (slide 13: fit preprocessing on train only).
    """
    profile = (train.groupby(["zone_id", "hour_of_day", "is_weekend"])["target_trips"]
               .mean().rename("train_profile"))
    out = pd.DataFrame(index=scored.index)
    out["same_hour_last_week"] = scored["trips_same_hour_lag_7d"]
    out["same_hour_yesterday"] = scored["trips_same_hour_lag_1d"]
    out["same_hour_mean_7d"] = scored["trips_same_hour_mean_7d"]
    out["train_profile"] = scored.join(profile, on=["zone_id", "hour_of_day", "is_weekend"])["train_profile"]
    return out


def score(table: pd.DataFrame, split: str) -> pd.DataFrame:
    train = table[table["split"] == "train"]
    scored = table[table["split"] == split]
    predictions = predict(train, scored)
    actual = scored["target_trips"].astype(float)
    rows = []
    for name in predictions.columns:
        error = predictions[name].astype(float) - actual
        rows.append({
            "baseline": name,
            "rows": int(error.notna().sum()),
            "mae": float(error.abs().mean()),
            "rmse": float(np.sqrt((error ** 2).mean())),
        })
    return pd.DataFrame(rows).sort_values("mae").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Score naive baselines on a feature snapshot.")
    parser.add_argument("--snapshot", help="Snapshot folder (default: latest under data/features/)")
    parser.add_argument("--split", default="validate", choices=["train", "validate", "test"])
    parser.add_argument("--final", action="store_true",
                        help="Required to score the test split. Use exactly once, at the end.")
    args = parser.parse_args()
    if args.split == "test" and not args.final:
        parser.error("the test split is touched once, at the very end: pass --final to confirm")

    snapshot_dir = Path(args.snapshot) if args.snapshot else latest_snapshot()
    table, manifest = load_snapshot(snapshot_dir)
    results = score(table, args.split)

    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / f"phase8_baselines_{args.split}.md"
    lines = [
        f"# Phase 8 baselines: {args.split} split",
        "",
        f"Snapshot `{manifest['version']}` (sha256 `{manifest['sha256'][:12]}...`), "
        f"target `{manifest['target_column']}` = pickups per zone per hour, "
        f"seed {manifest['config']['reproducibility']['seed']} (no randomness is used).",
        f"Regenerate with `python -m src.baseline --snapshot data/features/{manifest['version']}`.",
        "",
        "| Baseline | Rows scored | MAE (trips/hour) | RMSE |",
        "|---|---:|---:|---:|",
        *[f"| `{r.baseline}` | {r.rows:,} | {r.mae:.3f} | {r.rmse:.3f} |" for r in results.itertuples()],
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"written to {out_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
