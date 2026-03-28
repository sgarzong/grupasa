from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import shutil
from urllib.request import urlopen
import warnings

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
NEW_FORMAT_SHEET_NAME = "Registro Contenedores"
HEADER_SENTINELS: dict[str, list[str]] = {
    "Registro_Contenedores": ["id_contenedor", "pedido", "fecha_cas"],
    "Planif_Grupasa": ["id_contenedor", "fecha_descarga_planificada", "bodega"],
    "Planif_Galagans": ["id_contenedor", "plan_llegada_patio", "plan_devolucion_vacio"],
    "Status_Operativo": ["id_contenedor", "status_actual", "comentario"],
    "Control_Calidad": ["id_contenedor", "regla"],
}


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
    workbook = pd.ExcelFile(source_path)
    sheet_names = set(workbook.sheet_names)
    if NEW_FORMAT_SHEET_NAME in sheet_names:
        return _read_new_format_workbook(workbook)

    selected: dict[str, pd.DataFrame] = {}
    for sheet_name in REQUIRED_SHEETS + OPTIONAL_SHEETS:
        try:
            selected[sheet_name] = _read_sheet_with_detected_header(source_path, sheet_name)
        except ValueError:
            LOGGER.warning("La hoja %s no existe en el workbook", sheet_name)
    return selected


def detect_header_rows(source_path: Path, sheet_names: list[str]) -> dict[str, int]:
    workbook = pd.ExcelFile(source_path)
    if NEW_FORMAT_SHEET_NAME in set(workbook.sheet_names):
        preview = pd.read_excel(source_path, sheet_name=NEW_FORMAT_SHEET_NAME, header=None)
        header_row = _detect_header_row(preview, "Registro_Contenedores")
        return {
            "Registro_Contenedores": header_row,
            "Planif_Grupasa": header_row,
            "Planif_Galagans": header_row,
            "Status_Operativo": header_row,
        }

    headers: dict[str, int] = {}
    for sheet_name in sheet_names:
        try:
            preview = pd.read_excel(source_path, sheet_name=sheet_name, header=None)
            headers[sheet_name] = _detect_header_row(preview, sheet_name)
        except ValueError:
            continue
    return headers


def _copy_file(source_path: Path, latest_path: Path, archive_path: Path) -> None:
    shutil.copy2(source_path, latest_path)
    shutil.copy2(source_path, archive_path)


def _read_sheet_with_detected_header(source_path: Path, sheet_name: str) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning,
        )
        preview = pd.read_excel(source_path, sheet_name=sheet_name, header=None)
    header_row = _detect_header_row(preview, sheet_name)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning,
        )
        df = pd.read_excel(source_path, sheet_name=sheet_name, header=header_row)
    df = df.dropna(how="all")
    df.columns = [str(column).strip() for column in df.columns]
    return df


def _detect_header_row(preview: pd.DataFrame, sheet_name: str) -> int:
    expected_tokens = HEADER_SENTINELS.get(sheet_name, [])
    best_row = 0
    best_score = -1

    for index in range(min(len(preview), 15)):
        row_values = [str(value).strip().lower() for value in preview.iloc[index].tolist() if pd.notna(value)]
        score = sum(token in row_values for token in expected_tokens)
        if score > best_score:
            best_score = score
            best_row = index

    LOGGER.info("Header detectado para %s en fila %s", sheet_name, best_row + 1)
    return best_row


def _read_new_format_workbook(workbook: pd.ExcelFile) -> dict[str, pd.DataFrame]:
    LOGGER.info("Workbook detectado en formato consolidado: %s", NEW_FORMAT_SHEET_NAME)
    consolidated = pd.read_excel(workbook, sheet_name=NEW_FORMAT_SHEET_NAME)
    consolidated = consolidated.dropna(how="all")
    consolidated.columns = [str(column).strip() for column in consolidated.columns]

    return {
        "Registro_Contenedores": consolidated.copy(),
        "Planif_Grupasa": _build_new_format_plan_grupasa(consolidated),
        "Planif_Galagans": _build_new_format_plan_galagans(consolidated),
        "Status_Operativo": _build_new_format_status(consolidated),
    }


def _build_new_format_plan_grupasa(consolidated: pd.DataFrame) -> pd.DataFrame:
    return consolidated.rename(
        columns={
            "Fecha Descarga Planificada": "Plan_Llegada_Grupasa",
            "Comentario": "Comentario_Plan",
        }
    ).reindex(
        columns=[
            "ID_Contenedor",
            "Pedido",
            "Naviera",
            "Puerto",
            "Deposito_Vacio",
            "Fecha_CAS",
            "Plan_Llegada_Grupasa",
            "Bodega",
            "Hora_Descarga",
            "Comentario_Plan",
        ]
    )


def _build_new_format_plan_galagans(consolidated: pd.DataFrame) -> pd.DataFrame:
    plan = consolidated.rename(
        columns={
            "Fecha Retiro Puerto": "Plan_Llegada_Patio",
            "Fecha_Plan_Devolucion_Vacio": "Plan_Devolucion_Vacio",
            "Comentario": "Comentario_Plan",
        }
    ).reindex(
        columns=[
            "ID_Contenedor",
            "Pedido",
            "Naviera",
            "Puerto",
            "Deposito_Vacio",
            "Plan_Llegada_Patio",
            "Plan_Devolucion_Vacio",
            "Comentario_Plan",
        ]
    )
    return plan


def _build_new_format_status(consolidated: pd.DataFrame) -> pd.DataFrame:
    return consolidated.rename(
        columns={
            "Comentario": "Comentario",
        }
    ).reindex(
        columns=[
            "ID_Contenedor",
            "Pedido",
            "Naviera",
            "Puerto",
            "Fecha_CAS",
            "Fecha Descarga Planificada",
            "Fecha_Plan_Devolucion_Vacio",
            "Deposito_Vacio",
            "Status_Actual",
            "Horario_Entrega_Real",
            "Tipo_Incidencia",
            "Comentario",
        ]
    )
