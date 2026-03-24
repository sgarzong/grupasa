from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.protect_sheet import _build_protection_batches, extract_spreadsheet_id


def test_extract_spreadsheet_id() -> None:
    url = "https://docs.google.com/spreadsheets/d/1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX/export?format=xlsx"
    assert extract_spreadsheet_id(url) == "1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX"


def test_build_protection_batches_deletes_then_formats_then_protects() -> None:
    existing = [{"protectedRangeId": 11, "range": {"sheetId": 1, "startRowIndex": 3, "endRowIndex": 5}}]
    targets = [{"sheet_name": "Registro_Contenedores", "range": {"sheetId": 1, "startRowIndex": 3, "endRowIndex": 5}}]

    batches = _build_protection_batches(existing, targets)

    assert len(batches) == 3
    assert list(batches[0][0].keys()) == ["deleteProtectedRange"]
    assert list(batches[1][0].keys()) == ["repeatCell"]
    assert list(batches[2][0].keys()) == ["addProtectedRange"]
