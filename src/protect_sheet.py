from __future__ import annotations

import json
import logging
import re
from typing import Any

import pandas as pd

from src.config import Settings


LOGGER = logging.getLogger(__name__)
MANAGED_PROTECTION_PREFIX = "grupasa-pipeline-managed"
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

    service = _build_sheets_service(settings.google_service_account_json)
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing_protections = _collect_managed_protections(spreadsheet)
    requests: list[dict[str, Any]] = []

    for protection_id in existing_protections:
        requests.append({"deleteProtectedRange": {"protectedRangeId": protection_id}})

    sheet_meta_by_name = {
        sheet["properties"]["title"]: sheet["properties"]
        for sheet in spreadsheet.get("sheets", [])
    }

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

        requests.append(
            {
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_row_index,
                            "endRowIndex": end_row_index,
                            "startColumnIndex": 0,
                            "endColumnIndex": end_column_index,
                        },
                        "description": f"{MANAGED_PROTECTION_PREFIX}:{sheet_name}",
                        "warningOnly": False,
                    }
                }
            }
        )

    if not requests:
        LOGGER.info("No hay solicitudes de proteccion para enviar")
        return

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()
    LOGGER.info("Protecciones actualizadas en Google Sheets para %s hojas", len(TARGET_SHEETS))


def extract_spreadsheet_id(url: str) -> str | None:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    return None


def _build_sheets_service(service_account_json: str):
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    credentials_info = json.loads(service_account_json)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _collect_managed_protections(spreadsheet: dict[str, Any]) -> list[int]:
    protection_ids: list[int] = []
    for sheet in spreadsheet.get("sheets", []):
        for protection in sheet.get("protectedRanges", []):
            description = protection.get("description", "")
            if description.startswith(MANAGED_PROTECTION_PREFIX):
                protection_ids.append(protection["protectedRangeId"])
    return protection_ids
