from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.inventory_history import InventoryWorkbook, build_historical_outputs, synthesize_status_history
from src.transform import map_status_to_stage


def test_synthesize_status_history_expands_stage_dates_daily() -> None:
    inventory_df = pd.DataFrame(
        [
            {
                "contenedor_id": "MSCU1",
                "pedido": "PED1",
                "naviera": "MSC",
                "puerto": "TPG",
                "fecha_cas": date(2026, 1, 16),
                "plan_llegada_grupasa": date(2026, 1, 15),
                "plan_devolucion_vacio": date(2026, 1, 20),
                "deposito_vacio": "MEDLOG",
                "fecha_arribo_gye": date(2026, 1, 2),
                "fecha_salida_autorizada": date(2026, 1, 12),
                "fecha_retiro_puerto": date(2026, 1, 13),
                "fecha_traslado_planta": date(2026, 1, 14),
                "fecha_devolucion": date(2026, 1, 15),
                "bodega": "CAMPO CHINO",
                "tipo_incidencia": "SIN NOVEDAD",
            }
        ]
    )

    history = synthesize_status_history([InventoryWorkbook(Path("enero.xlsx"), date(2026, 1, 15), inventory_df)])

    assert set(history["status_actual"]) == {"EN PUERTO", "EN PATIO", "EN GRUPASA", "DEVUELTO DEPOSITO VACIO"}
    assert len(history) == 14
    assert history.iloc[0]["fecha_snapshot"] == "2026-01-02"
    assert history.iloc[-1]["fecha_snapshot"] == "2026-01-15"


def test_build_historical_outputs_builds_powerbi_outputs() -> None:
    inventory_df = pd.DataFrame(
        [
            {
                "contenedor_id": "MSCU1",
                "pedido": "PED1",
                "parcial": "P1",
                "naviera": "MSC",
                "puerto": "TPG",
                "deposito_vacio": "MEDLOG",
                "fecha_arribo_gye": date(2026, 1, 2),
                "fecha_salida_autorizada": date(2026, 1, 12),
                "fecha_retiro_puerto": date(2026, 1, 13),
                "fecha_cas": date(2026, 1, 16),
                "plan_llegada_grupasa": date(2026, 1, 15),
                "fecha_traslado_planta": date(2026, 1, 14),
                "bodega": "CAMPO CHINO",
                "fecha_devolucion": date(2026, 1, 15),
                "plan_devolucion_vacio": date(2026, 1, 15),
                "plan_llegada_patio": date(2026, 1, 13),
                "tipo_incidencia": "SIN NOVEDAD",
            }
        ]
    )

    outputs = build_historical_outputs([InventoryWorkbook(Path("enero.xlsx"), date(2026, 1, 15), inventory_df)], cas_alert_days=3)

    assert len(outputs["contenedores_actual"]) == 1
    assert len(outputs["status_historico"]) == 14
    assert len(outputs["fact_plan_actual"]) == 1
    assert len(outputs["fact_status_diario"]) == 14
    assert outputs["contenedores_actual"].iloc[0]["status_actual"] == "DEVUELTO DEPOSITO VACIO"


def test_map_status_to_stage_treats_grupasa_as_bodega() -> None:
    assert map_status_to_stage("EN GRUPASA") == "BODEGA"


def test_synthesize_status_history_caps_days_at_workbook_snapshot_date() -> None:
    inventory_df = pd.DataFrame(
        [
            {
                "contenedor_id": "MSCU2",
                "pedido": "PED2",
                "naviera": "MSC",
                "puerto": "TPG",
                "fecha_cas": date(2026, 3, 1),
                "plan_llegada_grupasa": pd.NaT,
                "plan_devolucion_vacio": pd.NaT,
                "deposito_vacio": "MEDLOG",
                "fecha_arribo_gye": date(2026, 2, 14),
                "fecha_salida_autorizada": date(2026, 2, 19),
                "fecha_retiro_puerto": date(2026, 2, 21),
                "fecha_traslado_planta": date(2026, 6, 2),
                "fecha_devolucion": pd.NaT,
                "bodega": "CAMPO CHINO",
                "tipo_incidencia": "SIN NOVEDAD",
            }
        ]
    )

    history = synthesize_status_history([InventoryWorkbook(Path("marzo.xlsx"), date(2026, 3, 27), inventory_df)])

    assert history["fecha_snapshot"].max() == "2026-03-27"
