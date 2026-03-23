from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import shutil
from urllib.request import urlopen

import pandas as pd

from src.config import Settings


LOGGER = logging.getLogger(__name__)

REQUIRED_SHEETS = [
    "Registro_Contenedores",
    "Planif_Grupasa",
    "Planif_Galagans",
    "Status_Operativo",
]
OPTIONAL_SHEETS = ["Control_Calidad"]


@dataclass(frozen=True)
class DownloadResult:
    source_path: Path | None
    warnings: list[str]


def fetch_source_workbook(settings: Settings) -> DownloadResult:
    warnings: list[str] = []
    if settings.source_local_path:
        if not settings.source_local_path.exists():
            raise FileNotFoundError(f"No existe SOURCE_LOCAL_PATH: {settings.source_local_path}")
        _copy_file(settings.source_local_path, settings.source_latest_path, settings.source_archive_path)
        LOGGER.info("Fuente cargada desde SOURCE_LOCAL_PATH=%s", settings.source_local_path)
        return DownloadResult(source_path=settings.source_latest_path, warnings=warnings)

    try:
        LOGGER.info("Descargando fuente desde Google Sheets")
        with urlopen(settings.source_xlsx_url, timeout=60) as response:
            payload = response.read()
        settings.source_latest_path.write_bytes(payload)
        settings.source_archive_path.write_bytes(payload)
        LOGGER.info("Fuente descargada en %s", settings.source_latest_path)
        return DownloadResult(source_path=settings.source_latest_path, warnings=warnings)
    except Exception as exc:
        LOGGER.exception("No se pudo descargar la fuente: %s", exc)
        warnings.append(f"download_failed: {exc}")
        if settings.source_latest_path.exists():
            warnings.append("using_cached_raw_file")
            LOGGER.warning("Se reutiliza el archivo raw cacheado: %s", settings.source_latest_path)
            return DownloadResult(source_path=settings.source_latest_path, warnings=warnings)
        return DownloadResult(source_path=None, warnings=warnings)


def read_source_sheets(source_path: Path) -> dict[str, pd.DataFrame]:
    workbook = pd.read_excel(source_path, sheet_name=None)
    selected: dict[str, pd.DataFrame] = {}
    for sheet_name in REQUIRED_SHEETS + OPTIONAL_SHEETS:
        if sheet_name in workbook:
            selected[sheet_name] = workbook[sheet_name].copy()
    return selected


def _copy_file(source_path: Path, latest_path: Path, archive_path: Path) -> None:
    shutil.copy2(source_path, latest_path)
    shutil.copy2(source_path, archive_path)
