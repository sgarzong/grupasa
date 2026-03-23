from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)


def append_daily_snapshot(
    current_df: pd.DataFrame,
    history_path: Path,
    snapshot_date: str,
    key_columns: list[str],
) -> pd.DataFrame:
    snapshot_df = current_df.copy()
    snapshot_df["fecha_snapshot"] = snapshot_date

    if history_path.exists():
        existing = pd.read_csv(history_path)
        existing = existing.reindex(columns=snapshot_df.columns)
    else:
        existing = pd.DataFrame(columns=snapshot_df.columns)

    if existing.empty:
        combined = snapshot_df.copy()
    elif snapshot_df.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, snapshot_df], ignore_index=True, sort=False)
    dedupe_keys = [column for column in ["fecha_snapshot", *key_columns] if column in combined.columns]
    if dedupe_keys:
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")

    combined.to_csv(history_path, index=False)
    LOGGER.info("Snapshot persistido: %s (%s filas)", history_path.name, len(combined))
    return combined


def upsert_latest_snapshot(
    current_df: pd.DataFrame,
    output_path: Path,
    snapshot_date: str,
    key_columns: list[str],
) -> pd.DataFrame:
    latest_df = current_df.copy()
    latest_df["fecha_snapshot"] = snapshot_date

    combined = latest_df.copy()
    dedupe_keys = [column for column in key_columns if column in combined.columns]
    if dedupe_keys:
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")

    combined.to_csv(output_path, index=False)
    LOGGER.info("Estado latest persistido: %s (%s filas)", output_path.name, len(combined))
    return combined
