from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.validate import standardize_and_validate


def test_validate_detects_business_rule_errors() -> None:
    sheets = {
        "Registro_Contenedores": pd.DataFrame(
            [
                {
                    "Contenedor": "MSCU1",
                    "Pedido": "PED1",
                    "Parcial": "P1",
                    "Naviera": "MSC",
                    "Puerto": "GYE",
                    "Deposito_Vacio": "DEP1",
                    "Fecha_Arribo": "2026-03-20",
                    "Fecha_CAS": None,
                }
            ]
        ),
        "Planif_Grupasa": pd.DataFrame(
            [
                {
                    "Contenedor": "MSCU1",
                    "Plan_Llegada_Grupasa": "2026-03-21",
                    "Bodega": "B1",
                    "Hora_Descarga": "08:00",
                    "Comentario_Plan": "Ok",
                }
            ]
        ),
        "Planif_Galagans": pd.DataFrame(
            [
                {
                    "Contenedor": "MSCU1",
                    "Plan_Llegada_Patio": "2026-03-20",
                    "Plan_Devolucion_Vacio": "2026-03-25",
                    "Comentario_Plan": "Ok",
                }
            ]
        ),
        "Status_Operativo": pd.DataFrame(
            [
                {
                    "Contenedor": "MSCU1",
                    "Status_Actual": "EN PUERTO",
                    "Horario_Entrega_Real": "10:00",
                    "Tipo_Incidencia": "",
                    "Comentario": "Error esperado",
                },
                {
                    "Contenedor": "MSCU1",
                    "Status_Actual": "ENTREGADO",
                    "Horario_Entrega_Real": "",
                    "Tipo_Incidencia": "",
                    "Comentario": "Duplicado",
                },
            ]
        ),
    }

    _, issues = standardize_and_validate(sheets, "2026-03-22")
    error_codes = set(issues["error_code"].tolist())

    assert "fecha_cas_vacia" in error_codes
    assert "duplicate_contenedor_id" in error_codes
    assert "horario_entrega_invalido" in error_codes
    assert "entregado_sin_horario" in error_codes
