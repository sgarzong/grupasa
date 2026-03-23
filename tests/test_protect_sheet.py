from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.protect_sheet import extract_spreadsheet_id


def test_extract_spreadsheet_id() -> None:
    url = "https://docs.google.com/spreadsheets/d/1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX/export?format=xlsx"
    assert extract_spreadsheet_id(url) == "1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX"
