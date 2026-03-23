from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd


LOGGER = logging.getLogger(__name__)


def export_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    LOGGER.info("Archivo exportado: %s (%s filas)", path, len(df))
