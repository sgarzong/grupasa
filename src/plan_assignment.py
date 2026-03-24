from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from src.transform import map_status_to_stage


ASSIGNMENT_OUTPUT_COLUMNS = [
    "fecha_snapshot",
    "pedido",
    "plan_slot",
    "contenedor_id",
    "fecha_primer_movimiento",
    "plan_llegada_grupasa",
    "bodega",
    "hora_descarga",
    "comentario_plan_grupasa",
    "tipo_asignacion",
]


def load_existing_assignments(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=ASSIGNMENT_OUTPUT_COLUMNS)
    try:
        existing = pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame(columns=ASSIGNMENT_OUTPUT_COLUMNS)
    for column in ASSIGNMENT_OUTPUT_COLUMNS:
        if column not in existing.columns:
            existing[column] = pd.NA
    return existing[ASSIGNMENT_OUTPUT_COLUMNS].copy()


def resolve_grupasa_assignments(
    registro_df: pd.DataFrame,
    plan_grupasa_df: pd.DataFrame,
    status_history_df: pd.DataFrame,
    existing_assignments_df: pd.DataFrame,
    snapshot_date: str,
) -> pd.DataFrame:
    slots = _build_plan_slots(plan_grupasa_df)
    movement_queue = _build_movement_queue(registro_df, status_history_df)

    if slots.empty and existing_assignments_df.empty:
        return pd.DataFrame(columns=ASSIGNMENT_OUTPUT_COLUMNS)

    assignments: list[dict[str, object]] = []
    existing = existing_assignments_df.copy()
    for column in ASSIGNMENT_OUTPUT_COLUMNS:
        if column not in existing.columns:
            existing[column] = pd.NA

    for pedido in sorted(set(slots["pedido"].dropna().astype(str)) | set(movement_queue["pedido"].dropna().astype(str))):
        pedido_slots = slots.loc[slots["pedido"].astype(str) == pedido].copy()
        pedido_movements = movement_queue.loc[movement_queue["pedido"].astype(str) == pedido].copy()
        pedido_existing = existing.loc[existing["pedido"].astype(str) == pedido].copy()

        used_slots: set[int] = set()
        used_containers: set[str] = set()

        for row in pedido_existing.sort_values(["plan_slot", "contenedor_id"]).itertuples(index=False):
            container_id = str(row.contenedor_id)
            slot_number = int(row.plan_slot)
            if container_id not in set(pedido_movements["contenedor_id"].astype(str)):
                continue
            if container_id in used_containers:
                continue
            used_containers.add(container_id)
            used_slots.add(slot_number)

            current_slot = pedido_slots.loc[pedido_slots["plan_slot"] == slot_number]
            if not current_slot.empty:
                slot_row = current_slot.iloc[0]
                assignments.append(
                    _build_assignment_record(
                        snapshot_date=snapshot_date,
                        pedido=pedido,
                        plan_slot=slot_number,
                        contenedor_id=container_id,
                        fecha_primer_movimiento=_lookup_first_movement(pedido_movements, container_id),
                        plan_row=slot_row.to_dict(),
                        tipo_asignacion="persistida",
                    )
                )
            else:
                assignments.append(
                    {
                        "fecha_snapshot": snapshot_date,
                        "pedido": pedido,
                        "plan_slot": slot_number,
                        "contenedor_id": container_id,
                        "fecha_primer_movimiento": _lookup_first_movement(pedido_movements, container_id),
                        "plan_llegada_grupasa": row.plan_llegada_grupasa,
                        "bodega": row.bodega,
                        "hora_descarga": row.hora_descarga,
                        "comentario_plan_grupasa": row.comentario_plan_grupasa,
                        "tipo_asignacion": "persistida_historica",
                    }
                )

        remaining_movements = pedido_movements.loc[~pedido_movements["contenedor_id"].astype(str).isin(used_containers)].copy()
        remaining_slots = pedido_slots.loc[~pedido_slots["plan_slot"].isin(used_slots)].copy()

        for movement_row, slot_row in zip(remaining_movements.itertuples(index=False), remaining_slots.itertuples(index=False)):
            assignments.append(
                _build_assignment_record(
                    snapshot_date=snapshot_date,
                    pedido=pedido,
                    plan_slot=int(slot_row.plan_slot),
                    contenedor_id=str(movement_row.contenedor_id),
                    fecha_primer_movimiento=movement_row.fecha_primer_movimiento,
                    plan_row=slot_row._asdict(),
                    tipo_asignacion="movimiento_real",
                )
            )

    assignment_df = pd.DataFrame(assignments)
    if assignment_df.empty:
        return pd.DataFrame(columns=ASSIGNMENT_OUTPUT_COLUMNS)

    assignment_df = assignment_df.sort_values(["pedido", "plan_slot", "fecha_primer_movimiento", "contenedor_id"]).drop_duplicates(
        subset=["pedido", "contenedor_id"], keep="last"
    )
    for column in ASSIGNMENT_OUTPUT_COLUMNS:
        if column not in assignment_df.columns:
            assignment_df[column] = pd.NA
    return assignment_df[ASSIGNMENT_OUTPUT_COLUMNS].reset_index(drop=True)


def apply_grupasa_assignments(
    plan_grupasa_df: pd.DataFrame,
    assignments_df: pd.DataFrame,
    moved_container_ids: set[str],
) -> pd.DataFrame:
    direct_plan = plan_grupasa_df[["contenedor_id", "plan_llegada_grupasa", "bodega", "hora_descarga", "comentario_plan_grupasa"]].copy()
    direct_plan["tipo_asignacion"] = "directa_hoja"
    direct_plan["plan_slot"] = pd.NA

    assignment_plan = assignments_df.rename(columns={"fecha_snapshot": "fecha_snapshot_asignacion"}).copy()
    assignment_plan = assignment_plan[
        ["contenedor_id", "plan_llegada_grupasa", "bodega", "hora_descarga", "comentario_plan_grupasa", "tipo_asignacion", "plan_slot"]
    ]

    combined = pd.concat(
        [
            direct_plan.loc[~direct_plan["contenedor_id"].astype(str).isin(moved_container_ids)],
            direct_plan.loc[direct_plan["contenedor_id"].astype(str).isin(moved_container_ids)],
            assignment_plan,
        ],
        ignore_index=True,
        sort=False,
    )
    combined = combined.drop_duplicates(subset=["contenedor_id"], keep="last")
    return combined.reset_index(drop=True)


def extract_moved_container_ids(status_history_df: pd.DataFrame) -> set[str]:
    movements = _build_movement_queue(pd.DataFrame(), status_history_df)
    return set(movements["contenedor_id"].astype(str))


def _build_plan_slots(plan_grupasa_df: pd.DataFrame) -> pd.DataFrame:
    if plan_grupasa_df.empty:
        return pd.DataFrame(columns=["pedido", "plan_slot", "plan_llegada_grupasa", "bodega", "hora_descarga", "comentario_plan_grupasa"])

    slots = plan_grupasa_df.copy().reset_index(drop=True)
    slots["_row_order"] = range(len(slots))
    slots["pedido"] = slots["pedido"].astype("string").str.strip()
    slots["hora_descarga_sort"] = pd.to_datetime(slots["hora_descarga"], format="%H:%M", errors="coerce")
    slots = slots.sort_values(["pedido", "plan_llegada_grupasa", "hora_descarga_sort", "_row_order"], na_position="last").reset_index(drop=True)
    slots["plan_slot"] = slots.groupby("pedido", dropna=False).cumcount() + 1
    return slots


def _build_movement_queue(registro_df: pd.DataFrame, status_history_df: pd.DataFrame) -> pd.DataFrame:
    if status_history_df.empty:
        return pd.DataFrame(columns=["pedido", "contenedor_id", "fecha_primer_movimiento"])

    history = status_history_df.copy()
    history["contenedor_id"] = history["contenedor_id"].astype("string").str.strip()
    history["pedido"] = history.get("pedido", pd.Series(dtype="string")).astype("string").str.strip()
    history["fecha_snapshot"] = pd.to_datetime(history["fecha_snapshot"], errors="coerce").dt.date
    history["stage"] = history["status_actual"].map(map_status_to_stage)
    movements = history.loc[history["stage"].isin(["PATIO", "BODEGA", "DEPOSITO"])].copy()

    if movements.empty:
        return pd.DataFrame(columns=["pedido", "contenedor_id", "fecha_primer_movimiento"])

    movement_queue = (
        movements.sort_values(["fecha_snapshot", "contenedor_id"])
        .groupby("contenedor_id", as_index=False)
        .agg({"fecha_snapshot": "first", "pedido": "first"})
        .rename(columns={"fecha_snapshot": "fecha_primer_movimiento"})
    )

    if not registro_df.empty:
        pedido_lookup = registro_df[["contenedor_id", "pedido"]].drop_duplicates(subset=["contenedor_id"]).copy()
        pedido_lookup["contenedor_id"] = pedido_lookup["contenedor_id"].astype("string").str.strip()
        pedido_lookup["pedido"] = pedido_lookup["pedido"].astype("string").str.strip()
        movement_queue = movement_queue.drop(columns=["pedido"]).merge(pedido_lookup, on="contenedor_id", how="left")

    movement_queue["pedido"] = movement_queue["pedido"].astype("string").str.strip()
    movement_queue = movement_queue.sort_values(["pedido", "fecha_primer_movimiento", "contenedor_id"]).reset_index(drop=True)
    return movement_queue


def _build_assignment_record(
    snapshot_date: str,
    pedido: str,
    plan_slot: int,
    contenedor_id: str,
    fecha_primer_movimiento: object,
    plan_row: pd.Series | dict[str, object],
    tipo_asignacion: str,
) -> dict[str, object]:
    return {
        "fecha_snapshot": snapshot_date,
        "pedido": pedido,
        "plan_slot": plan_slot,
        "contenedor_id": contenedor_id,
        "fecha_primer_movimiento": fecha_primer_movimiento,
        "plan_llegada_grupasa": plan_row["plan_llegada_grupasa"],
        "bodega": plan_row["bodega"],
        "hora_descarga": plan_row["hora_descarga"],
        "comentario_plan_grupasa": plan_row["comentario_plan_grupasa"],
        "tipo_asignacion": tipo_asignacion,
    }


def _lookup_first_movement(movement_queue: pd.DataFrame, contenedor_id: str) -> object:
    match = movement_queue.loc[movement_queue["contenedor_id"].astype(str) == str(contenedor_id), "fecha_primer_movimiento"]
    return match.iloc[0] if not match.empty else pd.NaT
