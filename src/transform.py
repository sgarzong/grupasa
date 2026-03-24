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
    "fecha_arribo_gye",
    "fecha_salida_autorizada",
    "fecha_arribo",
    "fecha_cas",
    "plan_llegada_grupasa",
    "bodega",
    "hora_descarga",
    "comentario_plan_grupasa",
    "plan_slot_grupasa",
    "tipo_asignacion_grupasa",
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

POWER_BI_TABLE_ORDER = [
    "dim_contenedor",
    "dim_fecha",
    "dim_status",
    "dim_bodega",
    "fact_status_diario",
    "fact_plan_actual",
]


def build_current_dataset(
    sheets: dict[str, pd.DataFrame],
    status_history: pd.DataFrame,
    grupasa_plan_resolved: pd.DataFrame,
    snapshot_date: str,
    cas_alert_days: int,
) -> pd.DataFrame:
    registro = sheets["Registro_Contenedores"].copy()
    plan_galagans = sheets["Planif_Galagans"].copy()
    status_actual = sheets["Status_Operativo"].copy()

    id_index = _build_id_index(registro, grupasa_plan_resolved, plan_galagans, status_actual)

    current = (
        id_index.merge(
            registro.reindex(
                columns=[
                    "contenedor_id",
                    "pedido",
                    "parcial",
                    "naviera",
                    "puerto",
                    "deposito_vacio",
                    "fecha_arribo_gye",
                    "fecha_salida_autorizada",
                    "fecha_arribo",
                    "fecha_cas",
                ]
            ),
            on="contenedor_id",
            how="left",
        )
        .merge(
            grupasa_plan_resolved.reindex(
                columns=[
                    "contenedor_id",
                    "plan_llegada_grupasa",
                    "bodega",
                    "hora_descarga",
                    "comentario_plan_grupasa",
                    "plan_slot",
                    "tipo_asignacion",
                ]
            ),
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
    current = current.rename(
        columns={
            "plan_slot": "plan_slot_grupasa",
            "tipo_asignacion": "tipo_asignacion_grupasa",
        }
    )

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


def build_powerbi_star_schema(
    current_dataset: pd.DataFrame,
    status_history: pd.DataFrame,
    cas_alert_days: int,
) -> dict[str, pd.DataFrame]:
    dim_contenedor = _build_dim_contenedor(current_dataset, status_history)
    dim_status = _build_dim_status(current_dataset, status_history)
    dim_bodega = _build_dim_bodega(current_dataset)
    dim_fecha = _build_dim_fecha(current_dataset, status_history)
    fact_status_diario = _build_fact_status_diario(
        current_dataset=current_dataset,
        status_history=status_history,
        cas_alert_days=cas_alert_days,
        dim_contenedor=dim_contenedor,
        dim_status=dim_status,
        dim_bodega=dim_bodega,
    )
    fact_plan_actual = _build_fact_plan_actual(
        current_dataset=current_dataset,
        dim_contenedor=dim_contenedor,
        dim_fecha=dim_fecha,
        dim_status=dim_status,
        dim_bodega=dim_bodega,
    )

    return {
        "dim_contenedor": dim_contenedor,
        "dim_fecha": dim_fecha,
        "dim_status": dim_status,
        "dim_bodega": dim_bodega,
        "fact_status_diario": fact_status_diario,
        "fact_plan_actual": fact_plan_actual,
    }


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


def _build_dim_contenedor(current_dataset: pd.DataFrame, status_history: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "contenedor_id",
        "pedido",
        "parcial",
        "naviera",
        "puerto",
        "deposito_vacio",
    ]
    dim = current_dataset[columns].copy()
    if "contenedor_id" in status_history.columns:
        history_dim = pd.DataFrame({"contenedor_id": status_history["contenedor_id"].astype("string")})
        for column in columns[1:]:
            history_dim[column] = pd.NA
        dim = pd.concat([dim, history_dim], ignore_index=True, sort=False)

    dim = dim.dropna(subset=["contenedor_id"]).drop_duplicates(subset=["contenedor_id"], keep="first").sort_values("contenedor_id")
    dim.insert(0, "contenedor_key", range(1, len(dim) + 1))
    return dim.reset_index(drop=True)


def _build_dim_status(current_dataset: pd.DataFrame, status_history: pd.DataFrame) -> pd.DataFrame:
    sources: list[pd.Series] = []
    if "status_actual" in current_dataset.columns:
        sources.append(current_dataset["status_actual"])
    if "status_actual" in status_history.columns:
        sources.append(status_history["status_actual"])

    values = pd.Series(dtype="string")
    for source in sources:
        values = pd.concat([values, source.astype("string")], ignore_index=True)

    dim = pd.DataFrame({"status_actual": values.fillna("").str.strip()})
    dim = dim.loc[dim["status_actual"].ne("")].drop_duplicates().sort_values("status_actual").reset_index(drop=True)
    dim.insert(0, "status_key", range(1, len(dim) + 1))
    dim["status_stage"] = dim["status_actual"].map(map_status_to_stage).fillna("OTRO")

    unknown = pd.DataFrame([{"status_key": 0, "status_actual": "SIN_STATUS", "status_stage": "SIN_STATUS"}])
    return pd.concat([unknown, dim], ignore_index=True)


def _build_dim_bodega(current_dataset: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({"bodega": current_dataset.get("bodega", pd.Series(dtype="string")).astype("string").fillna("").str.strip()})
    dim = dim.loc[dim["bodega"].ne("")].drop_duplicates().sort_values("bodega").reset_index(drop=True)
    dim.insert(0, "bodega_key", range(1, len(dim) + 1))
    unknown = pd.DataFrame([{"bodega_key": 0, "bodega": "SIN_BODEGA"}])
    return pd.concat([unknown, dim], ignore_index=True)


def _build_dim_fecha(current_dataset: pd.DataFrame, status_history: pd.DataFrame) -> pd.DataFrame:
    date_columns = [
        "fecha_snapshot",
        "fecha_arribo_gye",
        "fecha_salida_autorizada",
        "fecha_arribo",
        "fecha_cas",
        "plan_llegada_grupasa",
        "plan_llegada_patio",
        "plan_devolucion_vacio",
    ]
    date_values = pd.Series(dtype="object")

    for column in date_columns:
        if column in current_dataset.columns:
            date_values = pd.concat([date_values, pd.to_datetime(current_dataset[column], errors="coerce").dt.date], ignore_index=True)

    if "fecha_snapshot" in status_history.columns:
        date_values = pd.concat(
            [date_values, pd.to_datetime(status_history["fecha_snapshot"], errors="coerce").dt.date],
            ignore_index=True,
        )

    unique_dates = sorted({value for value in date_values.dropna().tolist()})
    dim = pd.DataFrame({"fecha": unique_dates})
    if dim.empty:
        return pd.DataFrame(
            columns=["fecha_key", "fecha", "anio", "mes_numero", "mes_nombre", "anio_mes", "trimestre", "semana_iso", "dia"]
        )

    dim["fecha_key"] = dim["fecha"].apply(lambda value: int(value.strftime("%Y%m%d")))
    dim["anio"] = dim["fecha"].apply(lambda value: value.year)
    dim["mes_numero"] = dim["fecha"].apply(lambda value: value.month)
    dim["mes_nombre"] = dim["fecha"].apply(lambda value: value.strftime("%B"))
    dim["anio_mes"] = dim["fecha"].apply(lambda value: value.strftime("%Y-%m"))
    dim["trimestre"] = dim["fecha"].apply(lambda value: f"Q{((value.month - 1) // 3) + 1}")
    dim["semana_iso"] = dim["fecha"].apply(lambda value: value.isocalendar().week)
    dim["dia"] = dim["fecha"].apply(lambda value: value.day)
    return dim[
        ["fecha_key", "fecha", "anio", "mes_numero", "mes_nombre", "anio_mes", "trimestre", "semana_iso", "dia"]
    ]


def _build_fact_status_diario(
    current_dataset: pd.DataFrame,
    status_history: pd.DataFrame,
    cas_alert_days: int,
    dim_contenedor: pd.DataFrame,
    dim_status: pd.DataFrame,
    dim_bodega: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "fecha_snapshot",
        "contenedor_id",
        "status_actual",
        "horario_entrega_real",
        "tipo_incidencia",
        "comentario_status",
    ]
    fact = status_history.reindex(columns=columns).copy()
    if fact.empty:
        return pd.DataFrame(
            columns=[
                "fecha_key",
                "contenedor_key",
                "status_key",
                "bodega_key",
                "fecha_snapshot",
                "contenedor_id",
                "status_actual",
                "status_stage",
                "horario_entrega_real",
                "tipo_incidencia",
                "comentario_status",
                "alerta_cas",
                "cas_vencido",
                "es_ultimo_status",
            ]
        )

    fact["fecha_snapshot"] = pd.to_datetime(fact["fecha_snapshot"], errors="coerce").dt.date
    fact["status_actual"] = fact["status_actual"].astype("string").fillna("").str.strip()
    fact = fact.merge(
        current_dataset[["contenedor_id", "fecha_cas", "bodega"]].drop_duplicates(subset=["contenedor_id"]),
        on="contenedor_id",
        how="left",
    )
    fact["alerta_cas"] = fact.apply(
        lambda row: _compute_alerta_cas(
            pd.Series({"status_actual": row.get("status_actual"), "fecha_cas": row.get("fecha_cas")}),
            row["fecha_snapshot"],
            cas_alert_days,
        )
        if pd.notna(row["fecha_snapshot"])
        else False,
        axis=1,
    )
    fact["cas_vencido"] = fact.apply(
        lambda row: _compute_cas_vencido(
            pd.Series({"status_actual": row.get("status_actual"), "fecha_cas": row.get("fecha_cas")}),
            row["fecha_snapshot"],
        )
        if pd.notna(row["fecha_snapshot"])
        else False,
        axis=1,
    )
    latest_dates = fact.groupby("contenedor_id", dropna=False)["fecha_snapshot"].transform("max")
    fact["es_ultimo_status"] = fact["fecha_snapshot"].eq(latest_dates)
    fact["status_stage"] = fact["status_actual"].map(map_status_to_stage).fillna("OTRO")

    fact = fact.merge(dim_contenedor[["contenedor_key", "contenedor_id"]], on="contenedor_id", how="left")
    fact = fact.merge(dim_status[["status_key", "status_actual"]], on="status_actual", how="left")
    fact = fact.merge(dim_bodega[["bodega_key", "bodega"]], on="bodega", how="left")
    fact["fecha_key"] = fact["fecha_snapshot"].apply(lambda value: int(value.strftime("%Y%m%d")) if pd.notna(value) else pd.NA)
    fact["status_key"] = fact["status_key"].fillna(0).astype("Int64")
    fact["bodega_key"] = fact["bodega_key"].fillna(0).astype("Int64")
    fact["contenedor_key"] = fact["contenedor_key"].astype("Int64")

    fact = fact.sort_values(["fecha_snapshot", "contenedor_id"]).reset_index(drop=True)
    return fact[
        [
            "fecha_key",
            "contenedor_key",
            "status_key",
            "bodega_key",
            "fecha_snapshot",
            "contenedor_id",
            "status_actual",
            "status_stage",
            "horario_entrega_real",
            "tipo_incidencia",
            "comentario_status",
            "alerta_cas",
            "cas_vencido",
            "es_ultimo_status",
        ]
    ]


def _build_fact_plan_actual(
    current_dataset: pd.DataFrame,
    dim_contenedor: pd.DataFrame,
    dim_fecha: pd.DataFrame,
    dim_status: pd.DataFrame,
    dim_bodega: pd.DataFrame,
) -> pd.DataFrame:
    fact = current_dataset.copy()
    if fact.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_fecha_key",
                "fecha_arribo_gye_key",
                "fecha_salida_autorizada_key",
                "fecha_arribo_key",
                "fecha_cas_key",
                "plan_llegada_grupasa_key",
                "plan_llegada_patio_key",
                "plan_devolucion_vacio_key",
                "contenedor_key",
                "status_key",
                "bodega_key",
                "fecha_snapshot",
                "contenedor_id",
                "pedido",
                "parcial",
                "naviera",
                "puerto",
                "deposito_vacio",
                "fecha_arribo_gye",
                "fecha_salida_autorizada",
                "fecha_arribo",
                "fecha_cas",
                "plan_llegada_grupasa",
                "hora_descarga",
                "plan_slot_grupasa",
                "tipo_asignacion_grupasa",
                "plan_llegada_patio",
                "plan_devolucion_vacio",
                "cumplimiento_grupasa",
                "cumplimiento_galagans",
                "dias_puerto_a_patio",
                "dias_patio_a_bodega",
                "dias_bodega_a_deposito",
                "alerta_cas",
                "cas_vencido",
            ]
        )

    fact = fact.merge(dim_contenedor[["contenedor_key", "contenedor_id"]], on="contenedor_id", how="left")
    fact = fact.merge(dim_status[["status_key", "status_actual"]], on="status_actual", how="left")
    fact = fact.merge(dim_bodega[["bodega_key", "bodega"]], on="bodega", how="left")

    for source_column, key_column in [
        ("fecha_snapshot", "snapshot_fecha_key"),
        ("fecha_arribo_gye", "fecha_arribo_gye_key"),
        ("fecha_salida_autorizada", "fecha_salida_autorizada_key"),
        ("fecha_arribo", "fecha_arribo_key"),
        ("fecha_cas", "fecha_cas_key"),
        ("plan_llegada_grupasa", "plan_llegada_grupasa_key"),
        ("plan_llegada_patio", "plan_llegada_patio_key"),
        ("plan_devolucion_vacio", "plan_devolucion_vacio_key"),
    ]:
        fact[key_column] = pd.to_datetime(fact[source_column], errors="coerce").dt.strftime("%Y%m%d")
        fact[key_column] = pd.to_numeric(fact[key_column], errors="coerce").astype("Int64")

    valid_date_keys = set(dim_fecha["fecha_key"].tolist()) if not dim_fecha.empty else set()
    for key_column in [
        "snapshot_fecha_key",
        "fecha_arribo_gye_key",
        "fecha_salida_autorizada_key",
        "fecha_arribo_key",
        "fecha_cas_key",
        "plan_llegada_grupasa_key",
        "plan_llegada_patio_key",
        "plan_devolucion_vacio_key",
    ]:
        fact[key_column] = fact[key_column].where(fact[key_column].isin(valid_date_keys))

    fact["status_key"] = fact["status_key"].fillna(0).astype("Int64")
    fact["bodega_key"] = fact["bodega_key"].fillna(0).astype("Int64")
    fact["contenedor_key"] = fact["contenedor_key"].astype("Int64")

    fact = fact.sort_values("contenedor_id").reset_index(drop=True)
    return fact[
        [
            "snapshot_fecha_key",
            "fecha_arribo_gye_key",
            "fecha_salida_autorizada_key",
            "fecha_arribo_key",
            "fecha_cas_key",
            "plan_llegada_grupasa_key",
            "plan_llegada_patio_key",
            "plan_devolucion_vacio_key",
            "contenedor_key",
            "status_key",
            "bodega_key",
            "fecha_snapshot",
            "contenedor_id",
            "pedido",
            "parcial",
            "naviera",
            "puerto",
            "deposito_vacio",
            "fecha_arribo_gye",
            "fecha_salida_autorizada",
            "fecha_arribo",
            "fecha_cas",
            "plan_llegada_grupasa",
            "hora_descarga",
            "plan_slot_grupasa",
            "tipo_asignacion_grupasa",
            "plan_llegada_patio",
            "plan_devolucion_vacio",
            "cumplimiento_grupasa",
            "cumplimiento_galagans",
            "dias_puerto_a_patio",
            "dias_patio_a_bodega",
            "dias_bodega_a_deposito",
            "alerta_cas",
            "cas_vencido",
        ]
    ]


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
