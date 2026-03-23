from __future__ import annotations

from datetime import date
import unicodedata

import pandas as pd


CURRENT_OUTPUT_COLUMNS = [
    "fecha_snapshot",
    "contenedor_id",
    "pedido",
    "parcial",
    "naviera",
    "puerto",
    "deposito_vacio",
    "fecha_arribo",
    "fecha_cas",
    "plan_llegada_grupasa",
    "bodega",
    "hora_descarga",
    "comentario_plan_grupasa",
    "plan_llegada_patio",
    "plan_devolucion_vacio",
    "comentario_plan_galagans",
    "status_actual",
    "horario_entrega_real",
    "tipo_incidencia",
    "comentario_status",
    "alerta_cas",
    "cas_vencido",
    "dias_puerto_a_patio",
    "dias_patio_a_bodega",
    "dias_bodega_a_deposito",
    "cumplimiento_grupasa",
    "cumplimiento_galagans",
]


def build_current_dataset(
    sheets: dict[str, pd.DataFrame],
    status_history: pd.DataFrame,
    snapshot_date: str,
    cas_alert_days: int,
) -> pd.DataFrame:
    registro = sheets["Registro_Contenedores"].copy()
    plan_grupasa = sheets["Planif_Grupasa"].copy()
    plan_galagans = sheets["Planif_Galagans"].copy()
    status_actual = sheets["Status_Operativo"].copy()

    id_index = _build_id_index(registro, plan_grupasa, plan_galagans, status_actual)

    current = (
        id_index.merge(
            registro[
                [
                    "contenedor_id",
                    "pedido",
                    "parcial",
                    "naviera",
                    "puerto",
                    "deposito_vacio",
                    "fecha_arribo",
                    "fecha_cas",
                ]
            ],
            on="contenedor_id",
            how="left",
        )
        .merge(
            plan_grupasa[
                [
                    "contenedor_id",
                    "plan_llegada_grupasa",
                    "bodega",
                    "hora_descarga",
                    "comentario_plan_grupasa",
                ]
            ],
            on="contenedor_id",
            how="left",
        )
        .merge(
            plan_galagans[
                [
                    "contenedor_id",
                    "plan_llegada_patio",
                    "plan_devolucion_vacio",
                    "comentario_plan_galagans",
                ]
            ],
            on="contenedor_id",
            how="left",
        )
        .merge(
            status_actual[
                [
                    "contenedor_id",
                    "status_actual",
                    "horario_entrega_real",
                    "tipo_incidencia",
                    "comentario_status",
                ]
            ],
            on="contenedor_id",
            how="left",
        )
    )

    history_metrics = derive_history_metrics(status_history, registro)
    current = current.merge(history_metrics, on="contenedor_id", how="left")
    current["fecha_snapshot"] = snapshot_date

    snapshot_ts = pd.to_datetime(snapshot_date).date()
    current["alerta_cas"] = current.apply(
        lambda row: _compute_alerta_cas(row, snapshot_ts, cas_alert_days),
        axis=1,
    )
    current["cas_vencido"] = current.apply(
        lambda row: _compute_cas_vencido(row, snapshot_ts),
        axis=1,
    )
    current["cumplimiento_grupasa"] = current.apply(_compute_cumplimiento_grupasa, axis=1)
    current["cumplimiento_galagans"] = current.apply(_compute_cumplimiento_galagans, axis=1)

    for column in CURRENT_OUTPUT_COLUMNS:
        if column not in current.columns:
            current[column] = pd.NA

    return current[CURRENT_OUTPUT_COLUMNS].sort_values("contenedor_id").reset_index(drop=True)


def derive_history_metrics(status_history: pd.DataFrame, registro_df: pd.DataFrame) -> pd.DataFrame:
    if status_history.empty:
        return pd.DataFrame(
            columns=[
                "contenedor_id",
                "dias_puerto_a_patio",
                "dias_patio_a_bodega",
                "dias_bodega_a_deposito",
                "fecha_primera_patio",
                "fecha_primera_bodega",
                "fecha_primera_deposito",
            ]
        )

    history = status_history.copy()
    history["fecha_snapshot"] = pd.to_datetime(history["fecha_snapshot"], errors="coerce").dt.date
    history["status_actual"] = history["status_actual"].astype("string").fillna("")
    history["stage"] = history["status_actual"].map(map_status_to_stage)

    arribo_lookup = registro_df[["contenedor_id", "fecha_arribo"]].copy()
    arribo_lookup["fecha_arribo"] = pd.to_datetime(arribo_lookup["fecha_arribo"], errors="coerce").dt.date

    stage_dates = (
        history.dropna(subset=["contenedor_id", "fecha_snapshot"])
        .groupby(["contenedor_id", "stage"], dropna=False)["fecha_snapshot"]
        .min()
        .unstack()
        .reset_index()
    )

    stage_dates = stage_dates.merge(arribo_lookup, on="contenedor_id", how="left")
    for column in ["PUERTO", "PATIO", "BODEGA", "DEPOSITO"]:
        if column not in stage_dates.columns:
            stage_dates[column] = pd.NaT

    stage_dates["fecha_origen_puerto"] = stage_dates["PUERTO"].combine_first(stage_dates["fecha_arribo"])
    stage_dates["dias_puerto_a_patio"] = _days_between(stage_dates["fecha_origen_puerto"], stage_dates["PATIO"])
    stage_dates["dias_patio_a_bodega"] = _days_between(stage_dates["PATIO"], stage_dates["BODEGA"])
    stage_dates["dias_bodega_a_deposito"] = _days_between(stage_dates["BODEGA"], stage_dates["DEPOSITO"])
    stage_dates = stage_dates.rename(
        columns={
            "PATIO": "fecha_primera_patio",
            "BODEGA": "fecha_primera_bodega",
            "DEPOSITO": "fecha_primera_deposito",
        }
    )

    return stage_dates[
        [
            "contenedor_id",
            "dias_puerto_a_patio",
            "dias_patio_a_bodega",
            "dias_bodega_a_deposito",
            "fecha_primera_patio",
            "fecha_primera_bodega",
            "fecha_primera_deposito",
        ]
    ]


def map_status_to_stage(status: object) -> str | None:
    normalized = _normalize_status_text(status)
    if not normalized:
        return None
    if "DEPOSITO" in normalized or "VACIO" in normalized:
        return "DEPOSITO"
    if "BODEGA" in normalized or "ENTREGADO" in normalized:
        return "BODEGA"
    if "PATIO" in normalized:
        return "PATIO"
    if "PUERTO" in normalized:
        return "PUERTO"
    return None


def evaluate_compliance(plan_date: date | None, actual_date: date | None) -> str:
    if plan_date is None:
        return "SIN_PLAN"
    if actual_date is None:
        return "PENDIENTE"
    return "CUMPLE" if actual_date <= plan_date else "INCUMPLE"


def _build_id_index(*dfs: pd.DataFrame) -> pd.DataFrame:
    ids = pd.Series(dtype="string")
    for df in dfs:
        if "contenedor_id" in df.columns:
            ids = pd.concat([ids, df["contenedor_id"].astype("string")], ignore_index=True)
    ids = ids.dropna().drop_duplicates().sort_values().reset_index(drop=True)
    return pd.DataFrame({"contenedor_id": ids})


def _compute_alerta_cas(row: pd.Series, snapshot_date: date, cas_alert_days: int) -> bool:
    status = _normalize_status_text(row.get("status_actual"))
    fecha_cas = _to_date(row.get("fecha_cas"))
    if status != "EN PUERTO" or fecha_cas is None:
        return False
    days_to_cas = (fecha_cas - snapshot_date).days
    return 0 <= days_to_cas <= cas_alert_days


def _compute_cas_vencido(row: pd.Series, snapshot_date: date) -> bool:
    status = _normalize_status_text(row.get("status_actual"))
    fecha_cas = _to_date(row.get("fecha_cas"))
    if status != "EN PUERTO" or fecha_cas is None:
        return False
    return fecha_cas < snapshot_date


def _compute_cumplimiento_grupasa(row: pd.Series) -> str:
    plan_date = _to_date(row.get("plan_llegada_grupasa"))
    actual_date = _to_date(row.get("fecha_primera_bodega"))
    return evaluate_compliance(plan_date, actual_date)


def _compute_cumplimiento_galagans(row: pd.Series) -> str:
    plan_devolucion = _to_date(row.get("plan_devolucion_vacio"))
    actual_deposito = _to_date(row.get("fecha_primera_deposito"))
    if plan_devolucion is not None:
        return evaluate_compliance(plan_devolucion, actual_deposito)
    return evaluate_compliance(_to_date(row.get("plan_llegada_patio")), _to_date(row.get("fecha_primera_patio")))


def _days_between(start: pd.Series, end: pd.Series) -> pd.Series:
    start_dt = pd.to_datetime(start, errors="coerce")
    end_dt = pd.to_datetime(end, errors="coerce")
    delta = (end_dt - start_dt).dt.days
    return delta.where(delta >= 0)


def _normalize_status_text(status: object) -> str:
    text = str(status or "").strip().upper()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.split())


def _to_date(value: object) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    converted = pd.to_datetime(value, errors="coerce")
    if pd.isna(converted):
        return None
    return converted.date()
