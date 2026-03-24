from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transform import build_current_dataset, build_powerbi_star_schema


def test_transform_builds_metrics_and_compliance() -> None:
    sheets = {
        "Registro_Contenedores": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "pedido": "PED-003",
                    "parcial": "P1",
                    "naviera": "OOCL",
                    "puerto": "Guayaquil",
                    "deposito_vacio": "Depo Norte",
                    "fecha_arribo": "2026-03-14",
                    "fecha_cas": "2026-03-21",
                }
            ]
        ),
        "Planif_Grupasa": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "plan_llegada_grupasa": "2026-03-18",
                    "bodega": "BOD-C",
                    "hora_descarga": "09:30",
                    "comentario_plan_grupasa": "Normal",
                }
            ]
        ),
        "Planif_Galagans": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "plan_llegada_patio": "2026-03-15",
                    "plan_devolucion_vacio": "2026-03-21",
                    "comentario_plan_galagans": "Patio 3",
                }
            ]
        ),
        "Status_Operativo": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "status_actual": "DEVUELTO DEPOSITO VACIO",
                    "horario_entrega_real": "",
                    "tipo_incidencia": "",
                    "comentario_status": "Ciclo completo",
                }
            ]
        ),
    }

    status_history = pd.DataFrame(
        [
            {"fecha_snapshot": "2026-03-14", "contenedor_id": "OOLU1111111", "status_actual": "EN PUERTO"},
            {"fecha_snapshot": "2026-03-15", "contenedor_id": "OOLU1111111", "status_actual": "EN PATIO"},
            {"fecha_snapshot": "2026-03-18", "contenedor_id": "OOLU1111111", "status_actual": "EN BODEGA"},
            {"fecha_snapshot": "2026-03-20", "contenedor_id": "OOLU1111111", "status_actual": "DEVUELTO DEPOSITO VACIO"},
        ]
    )

    grupasa_plan_resolved = sheets["Planif_Grupasa"].assign(plan_slot=1, tipo_asignacion="directa_hoja")
    current = build_current_dataset(sheets, status_history, grupasa_plan_resolved, "2026-03-22", 3)
    row = current.iloc[0]

    assert row["dias_puerto_a_patio"] == 1
    assert row["dias_patio_a_bodega"] == 3
    assert row["dias_bodega_a_deposito"] == 2
    assert row["cumplimiento_grupasa"] == "CUMPLE"
    assert row["cumplimiento_galagans"] == "CUMPLE"
    assert bool(row["cas_vencido"]) is False


def test_build_powerbi_star_schema_outputs_dimensions_and_facts() -> None:
    sheets = {
        "Registro_Contenedores": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "pedido": "PED-003",
                    "parcial": "P1",
                    "naviera": "OOCL",
                    "puerto": "Guayaquil",
                    "deposito_vacio": "Depo Norte",
                    "fecha_arribo": "2026-03-14",
                    "fecha_cas": "2026-03-21",
                }
            ]
        ),
        "Planif_Grupasa": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "plan_llegada_grupasa": "2026-03-18",
                    "bodega": "BOD-C",
                    "hora_descarga": "09:30",
                    "comentario_plan_grupasa": "Normal",
                }
            ]
        ),
        "Planif_Galagans": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "plan_llegada_patio": "2026-03-15",
                    "plan_devolucion_vacio": "2026-03-21",
                    "comentario_plan_galagans": "Patio 3",
                }
            ]
        ),
        "Status_Operativo": pd.DataFrame(
            [
                {
                    "contenedor_id": "OOLU1111111",
                    "status_actual": "EN BODEGA",
                    "horario_entrega_real": "",
                    "tipo_incidencia": "",
                    "comentario_status": "Sin novedad",
                }
            ]
        ),
    }

    status_history = pd.DataFrame(
        [
            {"fecha_snapshot": "2026-03-14", "contenedor_id": "OOLU1111111", "status_actual": "EN PUERTO"},
            {"fecha_snapshot": "2026-03-15", "contenedor_id": "OOLU1111111", "status_actual": "EN PATIO"},
            {"fecha_snapshot": "2026-03-18", "contenedor_id": "OOLU1111111", "status_actual": "EN BODEGA"},
        ]
    )

    grupasa_plan_resolved = sheets["Planif_Grupasa"].assign(plan_slot=1, tipo_asignacion="directa_hoja")
    current = build_current_dataset(sheets, status_history, grupasa_plan_resolved, "2026-03-18", 3)
    star = build_powerbi_star_schema(current, status_history, 3)

    assert list(star["dim_contenedor"]["contenedor_id"]) == ["OOLU1111111"]
    assert set(star["dim_status"]["status_actual"]) >= {"SIN_STATUS", "EN PUERTO", "EN PATIO", "EN BODEGA"}
    assert list(star["dim_bodega"]["bodega"]) == ["SIN_BODEGA", "BOD-C"]
    assert len(star["dim_fecha"]) >= 4
    assert len(star["fact_status_diario"]) == 3
    assert len(star["fact_plan_actual"]) == 1
    assert star["fact_plan_actual"].iloc[0]["contenedor_key"] == 1
    assert bool(star["fact_status_diario"].iloc[-1]["es_ultimo_status"]) is True
