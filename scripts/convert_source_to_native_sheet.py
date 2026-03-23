from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
import sys
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from src.protect_sheet import extract_spreadsheet_id


def build_services(service_account_json_path: Path):
    info = json.loads(service_account_json_path.read_text(encoding="utf-8"))
    drive_credentials = Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ],
    )
    drive = build("drive", "v3", credentials=drive_credentials, cache_discovery=False)
    return drive


def download_xlsx(source_url: str, output_path: Path) -> None:
    with urlopen(source_url, timeout=60) as response:
        output_path.write_bytes(response.read())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-account-json", required=True)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--name-suffix", default="Native Sheet")
    args = parser.parse_args()

    source_url = args.source_url
    source_id = extract_spreadsheet_id(source_url)
    if not source_id:
        raise SystemExit("No se pudo extraer spreadsheetId desde source-url")

    drive = build_services(Path(args.service_account_json))
    metadata = drive.files().get(
        fileId=source_id,
        fields="id,name,mimeType,parents,webViewLink",
        supportsAllDrives=True,
    ).execute()

    print("Source file:")
    print(json.dumps(metadata, indent=2, ensure_ascii=False))

    if metadata["mimeType"] == "application/vnd.google-apps.spreadsheet":
        print("\nEl documento ya es un Google Sheet nativo.")
        print(f"Editable URL: {metadata.get('webViewLink', '')}")
        print(f"Export URL: https://docs.google.com/spreadsheets/d/{metadata['id']}/export?format=xlsx")
        return 0

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "source.xlsx"
        download_xlsx(source_url, temp_path)

        media = MediaFileUpload(
            str(temp_path),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=False,
        )
        body = {
            "name": f"{metadata['name']} - {args.name_suffix}",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        if metadata.get("parents"):
            body["parents"] = metadata["parents"]

        created = drive.files().create(
            body=body,
            media_body=media,
            fields="id,name,mimeType,webViewLink",
            supportsAllDrives=True,
        ).execute()

    print("\nNuevo Google Sheet nativo creado:")
    print(json.dumps(created, indent=2, ensure_ascii=False))
    print(f"\nEditable URL: {created.get('webViewLink', '')}")
    print(f"Export URL: https://docs.google.com/spreadsheets/d/{created['id']}/export?format=xlsx")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
