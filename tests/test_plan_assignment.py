from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.plan_assignment import apply_grupasa_assignments, resolve_grupasa_assignments


def test_resolve_grupasa_assignments_uses_earliest_slots_for_first_movers() -> None:
    registro = pd.DataFrame(
        [
            {"contenedor_id": "CONT-A", "pedido": "PED-1"},
            {"contenedor_id": "CONT-B", "pedido": "PED-1"},
        ]
    )
    plan = pd.DataFrame(
        [
            {
                "contenedor_id": "CONT-A",
                "pedido": "PED-1",
                "plan_llegada_grupasa": "2026-03-10",
                "bodega": "BOD-1",
                "hora_descarga": "08:00",
                "comentario_plan_grupasa": "slot 1",
            },
            {
                "contenedor_id": "CONT-B",
                "pedido": "PED-1",
                "plan_llegada_grupasa": "2026-03-11",
                "bodega": "BOD-2",
                "hora_descarga": "10:00",
                "comentario_plan_grupasa": "slot 2",
            },
        ]
    )
    status_history = pd.DataFrame(
        [
            {"fecha_snapshot": "2026-03-09", "contenedor_id": "CONT-B", "pedido": "PED-1", "status_actual": "EN PATIO GALAGANS"},
            {"fecha_snapshot": "2026-03-10", "contenedor_id": "CONT-A", "pedido": "PED-1", "status_actual": "EN PATIO GALAGANS"},
        ]
    )

    assignments = resolve_grupasa_assignments(
        registro_df=registro,
        plan_grupasa_df=plan,
        status_history_df=status_history,
        existing_assignments_df=pd.DataFrame(),
        snapshot_date="2026-03-10",
    )
    resolved = apply_grupasa_assignments(plan, assignments, {"CONT-A", "CONT-B"}).sort_values("contenedor_id").reset_index(drop=True)

    assert assignments.loc[assignments["contenedor_id"] == "CONT-B", "plan_slot"].iloc[0] == 1
    assert resolved.loc[resolved["contenedor_id"] == "CONT-B", "plan_llegada_grupasa"].iloc[0] == "2026-03-10"
    assert resolved.loc[resolved["contenedor_id"] == "CONT-A", "plan_llegada_grupasa"].iloc[0] == "2026-03-11"


def test_resolve_grupasa_assignments_preserves_existing_container_assignment() -> None:
    registro = pd.DataFrame(
        [
            {"contenedor_id": "CONT-A", "pedido": "PED-1"},
            {"contenedor_id": "CONT-B", "pedido": "PED-1"},
        ]
    )
    plan = pd.DataFrame(
        [
            {
                "contenedor_id": "CONT-X",
                "pedido": "PED-1",
                "plan_llegada_grupasa": "2026-03-10",
                "bodega": "BOD-1",
                "hora_descarga": "08:00",
                "comentario_plan_grupasa": "slot 1",
            },
            {
                "contenedor_id": "CONT-Y",
                "pedido": "PED-1",
                "plan_llegada_grupasa": "2026-03-11",
                "bodega": "BOD-2",
                "hora_descarga": "10:00",
                "comentario_plan_grupasa": "slot 2",
            },
        ]
    )
    status_history = pd.DataFrame(
        [
            {"fecha_snapshot": "2026-03-09", "contenedor_id": "CONT-A", "pedido": "PED-1", "status_actual": "EN PATIO GALAGANS"},
            {"fecha_snapshot": "2026-03-10", "contenedor_id": "CONT-B", "pedido": "PED-1", "status_actual": "EN PATIO GALAGANS"},
        ]
    )
    existing_assignments = pd.DataFrame(
        [
            {
                "fecha_snapshot": "2026-03-09",
                "pedido": "PED-1",
                "plan_slot": 1,
                "contenedor_id": "CONT-B",
                "fecha_primer_movimiento": "2026-03-10",
                "plan_llegada_grupasa": "2026-03-10",
                "bodega": "BOD-1",
                "hora_descarga": "08:00",
                "comentario_plan_grupasa": "slot 1",
                "tipo_asignacion": "movimiento_real",
            }
        ]
    )

    assignments = resolve_grupasa_assignments(
        registro_df=registro,
        plan_grupasa_df=plan,
        status_history_df=status_history,
        existing_assignments_df=existing_assignments,
        snapshot_date="2026-03-10",
    )

    assert assignments.loc[assignments["contenedor_id"] == "CONT-B", "plan_slot"].iloc[0] == 1
