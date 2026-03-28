from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.protect_sheet import (
    _build_protection_batches,
    _build_target_ranges_for_sheet,
    extract_spreadsheet_id,
)


def test_extract_spreadsheet_id() -> None:
    url = "https://docs.google.com/spreadsheets/d/1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX/export?format=xlsx"
    assert extract_spreadsheet_id(url) == "1v0E_F2QbjWOGOR93vWT_hZEeNnK06CVX"


def test_build_protection_batches_deletes_then_formats_then_protects() -> None:
    existing = [{"protectedRangeId": 11, "range": {"sheetId": 1, "startRowIndex": 3, "endRowIndex": 5}}]
    targets = [{"sheet_name": "Registro_Contenedores", "range": {"sheetId": 1, "startRowIndex": 3, "endRowIndex": 5}}]

    batches = _build_protection_batches(existing, targets, "svc@example.com")

    assert len(batches) == 3
    assert list(batches[0][0].keys()) == ["deleteProtectedRange"]
    assert list(batches[1][0].keys()) == ["repeatCell"]
    assert list(batches[2][0].keys()) == ["addProtectedRange"]
    protected_range = batches[2][0]["addProtectedRange"]["protectedRange"]
    assert protected_range["editors"]["users"] == ["svc@example.com"]


def test_build_target_ranges_for_new_format_only_locks_filled_a_to_o_and_pr_when_entregado() -> None:
    df = pd.DataFrame(
        [
            {
                "ID_Contenedor": "ABC123",
                "Pedido": "P1",
                "Parcial": "",
                "BL": "BL1",
                "Naviera": "",
                "Tipo_Contenedor": "20",
                "Puerto": "CGSA",
                "Fecha_Arribo_ de la carga al puerto": "",
                "Fecha_Salida_Autorizada": "2026-03-28",
                "Fecha_CAS": "2026-03-30",
                "Deposito_Vacio": "",
                "Fecha Retiro Puerto": "",
                "Fecha Descarga Planificada": "",
                "Bodega": "Sierra",
                "Fecha_Plan_Devolucion_Vacio": "",
                "Status_Actual": "EN PUERTO",
                "Tipo_Incidencia": "",
                "Comentario": "Pendiente",
            },
            {
                "ID_Contenedor": "XYZ999",
                "Pedido": "P2",
                "Parcial": "1",
                "BL": "BL2",
                "Naviera": "MSC",
                "Tipo_Contenedor": "40",
                "Puerto": "CGSA",
                "Fecha_Arribo_ de la carga al puerto": "2026-03-25",
                "Fecha_Salida_Autorizada": "2026-03-27",
                "Fecha_CAS": "2026-03-29",
                "Deposito_Vacio": "Blasti",
                "Fecha Retiro Puerto": "2026-03-28",
                "Fecha Descarga Planificada": "2026-03-29",
                "Bodega": "Naranjal",
                "Fecha_Plan_Devolucion_Vacio": "2026-03-31",
                "Status_Actual": "ENTREGADO",
                "Tipo_Incidencia": "SIN NOVEDAD",
                "Comentario": "Cerrado",
            },
        ]
    )

    targets = _build_target_ranges_for_sheet("Registro Contenedores", df, 7, header_row=0)
    ranges = [target["range"] for target in targets]

    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 2} in ranges
    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 3, "endColumnIndex": 4} in ranges
    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 5, "endColumnIndex": 7} in ranges
    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 8, "endColumnIndex": 10} in ranges
    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 13, "endColumnIndex": 14} in ranges
    assert {"sheetId": 7, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 15, "endColumnIndex": 18} not in ranges
    assert {"sheetId": 7, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 0, "endColumnIndex": 15} in ranges
    assert {"sheetId": 7, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 15, "endColumnIndex": 18} in ranges
