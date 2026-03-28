from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.download_source import _detect_header_row, detect_header_rows, read_source_sheets


def test_detect_header_row_skips_intro_rows() -> None:
    preview = pd.DataFrame(
        [
            ["1. REGISTRO DE CONTENEDORES", None, None],
            [None, None, None],
            ["ID_Contenedor", "Pedido", "Fecha_CAS"],
            ["MSCU4510021", "PED-260401", "2026-04-15"],
        ]
    )

    assert _detect_header_row(preview, "Registro_Contenedores") == 2


def test_read_source_sheets_supports_new_consolidated_format() -> None:
    workbook_path = Path(__file__).resolve().parent / "_tmp_consolidated.xlsx"
    consolidated = pd.DataFrame(
        [
            {
                "ID_Contenedor": "MSCU4510021",
                "Pedido": "PED-260401",
                "Parcial": "P1",
                "BL": "BL1",
                "Naviera": "MSC",
                "Tipo_Contenedor": "40",
                "Puerto": "CGSA",
                "Fecha_Arribo_ de la carga al puerto": "2026-04-10",
                "Fecha_Salida_Autorizada": "2026-04-11",
                "Fecha_CAS": "2026-04-15",
                "Deposito_Vacio": "BLASTI",
                "Fecha Retiro Puerto": "2026-04-12",
                "Fecha Descarga Planificada": "2026-04-13",
                "Bodega": "B1",
                "Fecha_Plan_Devolucion_Vacio": "2026-04-18",
                "Status_Actual": "EN PUERTO",
                "Tipo_Incidencia": "SIN NOVEDAD",
                "Comentario": "Observacion",
            }
        ]
    )

    try:
        with pd.ExcelWriter(workbook_path) as writer:
            consolidated.to_excel(writer, index=False, sheet_name="Registro Contenedores")

        sheets = read_source_sheets(workbook_path)
        header_rows = detect_header_rows(workbook_path, ["Registro_Contenedores"])

        assert set(sheets) >= {"Registro_Contenedores", "Planif_Grupasa", "Planif_Galagans", "Status_Operativo"}
        assert header_rows["Registro_Contenedores"] == 0
        assert sheets["Registro_Contenedores"].iloc[0]["Fecha Retiro Puerto"] == "2026-04-12"
        assert sheets["Planif_Grupasa"].iloc[0]["Plan_Llegada_Grupasa"] == "2026-04-13"
        assert sheets["Planif_Galagans"].iloc[0]["Plan_Llegada_Patio"] == "2026-04-12"
        assert sheets["Status_Operativo"].iloc[0]["Status_Actual"] == "EN PUERTO"
    finally:
        try:
            workbook_path.unlink(missing_ok=True)
        except PermissionError:
            pass
