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

    target_ranges: list[dict[str, Any]] = []
    for sheet_name in TARGET_SHEETS:
        if sheet_name not in sheets or sheet_name not in sheet_meta_by_name:
            continue
        df = sheets[sheet_name]
        if df.empty:
            continue

        header_row = header_rows.get(sheet_name, 0)
        start_row_index = header_row + 1
        end_row_index = header_row + 1 + len(df)
        end_column_index = len(df.columns)
        sheet_id = sheet_meta_by_name[sheet_name]["sheetId"]
        target_range = {
            "sheetId": sheet_id,
            "startRowIndex": start_row_index,
            "endRowIndex": end_row_index,
            "startColumnIndex": 0,
            "endColumnIndex": end_column_index,
        }
        target_ranges.append({"sheet_name": sheet_name, "range": target_range})

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
