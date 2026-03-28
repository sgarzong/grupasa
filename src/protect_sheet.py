from __future__ import annotations

import json
import logging
import re
from typing import Any

import pandas as pd

from src.config import Settings


LOGGER = logging.getLogger(__name__)
MANAGED_PROTECTION_PREFIX = "grupasa-pipeline-managed"
LOCKED_BACKGROUND_COLOR = {"red": 0.85, "green": 0.85, "blue": 0.85}
LOCKED_TEXT_COLOR = {"red": 0.33, "green": 0.33, "blue": 0.33}
DEFAULT_BACKGROUND_COLOR = {"red": 1.0, "green": 1.0, "blue": 1.0}
DEFAULT_TEXT_COLOR = {"red": 0.0, "green": 0.0, "blue": 0.0}
TARGET_SHEETS = [
    "Registro_Contenedores",
    "Planif_Grupasa",
    "Planif_Galagans",
]
LEGACY_FULL_ROW_SHEETS = {
    "registro_contenedores",
    "planif_grupasa",
    "planif_galagans",
}


def protect_operational_rows(
    settings: Settings,
    sheets: dict[str, pd.DataFrame],
    header_rows: dict[str, int],
) -> None:
    if not settings.google_sheets_enable_protection:
        LOGGER.info("Proteccion en Google Sheets deshabilitada por configuracion")
        return
    if not settings.google_service_account_json:
        LOGGER.warning("Proteccion habilitada pero GOOGLE_SERVICE_ACCOUNT_JSON esta vacio")
        return

    spreadsheet_id = extract_spreadsheet_id(settings.source_xlsx_url)
    if not spreadsheet_id:
        LOGGER.warning("No se pudo extraer spreadsheet_id desde SOURCE_XLSX_URL")
        return

    credentials_info = json.loads(settings.google_service_account_json)
    service_account_email = credentials_info.get("client_email", "").strip()
    service = _build_sheets_service(credentials_info)
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_protections = _collect_managed_protections(spreadsheet)
    sheet_meta_by_name = {
        sheet["properties"]["title"]: sheet["properties"]
        for sheet in spreadsheet.get("sheets", [])
    }
    sheet_meta_by_normalized_name = {
        _normalize_sheet_name(name): properties
        for name, properties in sheet_meta_by_name.items()
    }

    target_ranges: list[dict[str, Any]] = []
    for sheet_name, df in sheets.items():
        normalized_sheet_name = _normalize_sheet_name(sheet_name)
        if normalized_sheet_name not in LEGACY_FULL_ROW_SHEETS:
            continue

        sheet_properties = sheet_meta_by_name.get(sheet_name) or sheet_meta_by_normalized_name.get(normalized_sheet_name)
        if not sheet_properties:
            continue
        if df.empty:
            continue

        header_row = header_rows.get(sheet_name, 0)
        sheet_id = sheet_properties["sheetId"]
        target_ranges.extend(_build_target_ranges_for_sheet(sheet_name, df, sheet_id, header_row))

    batches = _build_protection_batches(existing_protections, target_ranges, service_account_email)
    if not batches:
        LOGGER.info("No hay solicitudes de proteccion para enviar")
        return

    try:
        for requests in batches:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ).execute()
    except Exception as exc:
        raise RuntimeError(
            "No se pudieron actualizar las protecciones. "
            "Si existen protecciones previas creadas manualmente o por otro usuario, "
            "borralas una sola vez desde Google Sheets y vuelve a correr el pipeline. "
            f"Detalle original: {exc}"
        ) from exc
    LOGGER.info("Protecciones actualizadas en Google Sheets para %s hojas", len(TARGET_SHEETS))


def _build_target_ranges_for_sheet(
    sheet_name: str,
    df: pd.DataFrame,
    sheet_id: int,
    header_row: int,
) -> list[dict[str, Any]]:
    normalized_sheet_name = _normalize_sheet_name(sheet_name)
    if _is_new_format_sheet(normalized_sheet_name, df):
        return _build_new_format_target_ranges(sheet_name, df, sheet_id, header_row)
    if normalized_sheet_name in LEGACY_FULL_ROW_SHEETS:
        return [
            {
                "sheet_name": sheet_name,
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row + 1,
                    "endRowIndex": header_row + 1 + len(df),
                    "startColumnIndex": 0,
                    "endColumnIndex": len(df.columns),
                },
            }
        ]
    return []


def _build_new_format_target_ranges(
    sheet_name: str,
    df: pd.DataFrame,
    sheet_id: int,
    header_row: int,
) -> list[dict[str, Any]]:
    target_ranges: list[dict[str, Any]] = []
    normalized_columns = [_normalize_header_name(column) for column in df.columns]
    status_col_index = _find_column_index(normalized_columns, {"status_actual"})

    for row_index, row in enumerate(df.itertuples(index=False), start=header_row + 1):
        row_values = list(row)
        for start_col, end_col in _contiguous_non_empty_ranges(row_values[:15], max_columns=15):
            target_ranges.append(
                {
                    "sheet_name": sheet_name,
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_index,
                        "endRowIndex": row_index + 1,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                }
            )

        status_value = ""
        if status_col_index is not None and status_col_index < len(row_values):
            status_value = str(row_values[status_col_index]).strip().upper()
        if status_value == "ENTREGADO":
            end_column_index = min(18, len(df.columns))
            if end_column_index > 15:
                target_ranges.append(
                    {
                        "sheet_name": sheet_name,
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index,
                            "endRowIndex": row_index + 1,
                            "startColumnIndex": 15,
                            "endColumnIndex": end_column_index,
                        },
                    }
                )

    return target_ranges


def extract_spreadsheet_id(url: str) -> str | None:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    return None


def _build_sheets_service(credentials_info: dict[str, Any]):
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _normalize_sheet_name(value: object) -> str:
    return str(value).strip().lower().replace(" ", "_")


def _normalize_header_name(value: object) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _find_column_index(columns: list[str], candidates: set[str]) -> int | None:
    for index, column in enumerate(columns):
        if column in candidates:
            return index
    return None


def _is_new_format_sheet(normalized_sheet_name: str, df: pd.DataFrame) -> bool:
    if normalized_sheet_name != "registro_contenedores":
        return False

    normalized_columns = {_normalize_header_name(column) for column in df.columns}
    return {
        "status_actual",
        "fecha_descarga_planificada",
        "fecha_plan_devolucion_vacio",
    }.issubset(normalized_columns)


def _contiguous_non_empty_ranges(values: list[Any], max_columns: int) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    start_index: int | None = None

    for index, value in enumerate(values[:max_columns]):
        has_value = pd.notna(value) and str(value).strip() != ""
        if has_value and start_index is None:
            start_index = index
        elif not has_value and start_index is not None:
            ranges.append((start_index, index))
            start_index = None

    if start_index is not None:
        ranges.append((start_index, min(len(values), max_columns)))

    return ranges


def _collect_managed_protections(spreadsheet: dict[str, Any]) -> list[int]:
    protections: list[dict[str, Any]] = []
    for sheet in spreadsheet.get("sheets", []):
        for protection in sheet.get("protectedRanges", []):
            description = protection.get("description", "")
            if description.startswith(MANAGED_PROTECTION_PREFIX):
                protections.append(protection)
    return protections


def _build_protection_batches(
    existing_protections: list[dict[str, Any]],
    target_ranges: list[dict[str, Any]],
    service_account_email: str,
) -> list[list[dict[str, Any]]]:
    delete_requests: list[dict[str, Any]] = []
    reset_requests: list[dict[str, Any]] = []
    lock_format_requests: list[dict[str, Any]] = []
    add_protection_requests: list[dict[str, Any]] = []

    for protection in existing_protections:
        protection_range = protection.get("range", {})
        if protection_range:
            reset_requests.append(_build_format_request(protection_range, DEFAULT_BACKGROUND_COLOR, DEFAULT_TEXT_COLOR))
        delete_requests.append({"deleteProtectedRange": {"protectedRangeId": protection["protectedRangeId"]}})

    for target in target_ranges:
        target_range = target["range"]
        lock_format_requests.append(_build_format_request(target_range, LOCKED_BACKGROUND_COLOR, LOCKED_TEXT_COLOR))
        add_protection_requests.append(
            {
                "addProtectedRange": {
                    "protectedRange": {
                        "range": target_range,
                        "description": f"{MANAGED_PROTECTION_PREFIX}:{target['sheet_name']}",
                        "warningOnly": False,
                        "editors": {
                            "users": [service_account_email] if service_account_email else [],
                        },
                    }
                }
            }
        )

    batches = [batch for batch in [delete_requests, reset_requests + lock_format_requests, add_protection_requests] if batch]
    return batches


def _build_format_request(
    target_range: dict[str, Any],
    background_color: dict[str, float],
    text_color: dict[str, float],
) -> dict[str, Any]:
    return {
        "repeatCell": {
            "range": target_range,
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": background_color,
                    "textFormat": {
                        "foregroundColor": text_color,
                    },
                }
            },
            "fields": "userEnteredFormat(backgroundColor,textFormat.foregroundColor)",
        }
    }
