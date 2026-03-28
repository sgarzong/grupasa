from __future__ import annotations

from dataclasses import dataclass
import unicodedata

import pandas as pd


SHEET_ORDER = [
    "Registro_Contenedores",
    "Planif_Grupasa",
    "Planif_Galagans",
    "Status_Operativo",
    "Control_Calidad",
]

REQUIRED_COLUMNS: dict[str, list[str]] = {
    "Registro_Contenedores": [
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
    ],
    "Planif_Grupasa": [
        "contenedor_id",
        "plan_llegada_grupasa",
        "bodega",
        "hora_descarga",
        "comentario_plan_grupasa",
    ],
    "Planif_Galagans": [
        "contenedor_id",
        "plan_llegada_patio",
        "plan_devolucion_vacio",
        "comentario_plan_galagans",
    ],
    "Status_Operativo": [
        "contenedor_id",
        "status_actual",
        "tipo_incidencia",
        "comentario_status",
    ],
}

ALL_CANONICAL_COLUMNS: dict[str, list[str]] = {
    **REQUIRED_COLUMNS,
    "Control_Calidad": ["contenedor_id", "regla", "detalle"],
}

ALIASES_BY_SHEET: dict[str, dict[str, str]] = {
    "Registro_Contenedores": {
        "contenedor": "contenedor_id",
        "contenedor_id": "contenedor_id",
        "id_contenedor": "contenedor_id",
        "container": "contenedor_id",
        "container_id": "contenedor_id",
        "pedido": "pedido",
        "parcial": "parcial",
        "bl": "bl",
        "naviera": "naviera",
        "tipo_contenedor": "tipo_contenedor",
        "producto": "producto",
        "puerto": "puerto",
        "puerto_operacion": "puerto",
        "deposito_vacio": "deposito_vacio",
        "deposito_de_vacio": "deposito_vacio",
        "fecha_arribo_gye": "fecha_arribo_gye",
        "fecha_arribo_de_la_carga_al_puerto": "fecha_arribo_gye",
        "fecha_salida_autorizada": "fecha_salida_autorizada",
        "fecha_arribo": "fecha_arribo",
        "fecha_de_retiro_del_puerto_de_los_contenedores": "fecha_arribo",
        "fecha_retiro_puerto": "fecha_arribo",
        "arribo": "fecha_arribo",
        "fecha_cas": "fecha_cas",
        "cas": "fecha_cas",
    },
    "Planif_Grupasa": {
        "contenedor": "contenedor_id",
        "contenedor_id": "contenedor_id",
        "id_contenedor": "contenedor_id",
        "plan_llegada_grupasa": "plan_llegada_grupasa",
        "plan_llegada": "plan_llegada_grupasa",
        "fecha_descarga_planificada": "plan_llegada_grupasa",
        "fecha_cas": "fecha_cas",
        "pedido": "pedido",
        "naviera": "naviera",
        "puerto": "puerto",
        "deposito_vacio": "deposito_vacio",
        "bodega": "bodega",
        "hora_descarga": "hora_descarga",
        "comentario_plan": "comentario_plan_grupasa",
        "comentario_plan_grupasa": "comentario_plan_grupasa",
        "comentario": "comentario_plan_grupasa",
    },
    "Planif_Galagans": {
        "contenedor": "contenedor_id",
        "contenedor_id": "contenedor_id",
        "id_contenedor": "contenedor_id",
        "pedido": "pedido",
        "naviera": "naviera",
        "puerto": "puerto",
        "plan_llegada_grupasa": "plan_llegada_grupasa",
        "hora_descarga": "hora_descarga",
        "deposito_vacio": "deposito_vacio",
        "plan_llegada_patio": "plan_llegada_patio",
        "fecha_retiro_puerto": "plan_llegada_patio",
        "fecha_plan_devolucion_vacio": "plan_devolucion_vacio",
        "plan_devolucion_vacio": "plan_devolucion_vacio",
        "comentario_plan": "comentario_plan_galagans",
        "comentario_plan_galagans": "comentario_plan_galagans",
        "comentario": "comentario_plan_galagans",
    },
    "Status_Operativo": {
        "contenedor": "contenedor_id",
        "contenedor_id": "contenedor_id",
        "id_contenedor": "contenedor_id",
        "pedido": "pedido",
        "naviera": "naviera",
        "puerto": "puerto",
        "fecha_cas": "fecha_cas",
        "plan_llegada_grupasa": "plan_llegada_grupasa",
        "fecha_descarga_planificada": "plan_llegada_grupasa",
        "hora_descarga": "hora_descarga",
        "fecha_plan_devolucion_vacio": "plan_devolucion_vacio",
        "plan_devolucion_vacio": "plan_devolucion_vacio",
        "deposito_vacio": "deposito_vacio",
        "status_actual": "status_actual",
        "status": "status_actual",
        "horario_entrega_real": "horario_entrega_real",
        "horario_entrega_grupasa_real": "horario_entrega_real",
        "tipo_incidencia": "tipo_incidencia",
        "comentario": "comentario_status",
        "comentario_status": "comentario_status",
    },
    "Control_Calidad": {
        "contenedor": "contenedor_id",
        "contenedor_id": "contenedor_id",
        "id_contenedor": "contenedor_id",
        "regla": "regla",
        "detalle": "detalle",
    },
}

DATE_COLUMNS = {
    "Registro_Contenedores": ["fecha_arribo_gye", "fecha_salida_autorizada", "fecha_arribo", "fecha_cas"],
    "Planif_Grupasa": ["fecha_cas", "plan_llegada_grupasa"],
    "Planif_Galagans": ["plan_llegada_grupasa", "plan_llegada_patio", "plan_devolucion_vacio"],
    "Status_Operativo": ["fecha_cas", "plan_llegada_grupasa", "plan_devolucion_vacio"],
}

TEXT_COLUMNS = {
    "Status_Operativo": ["status_actual", "tipo_incidencia", "comentario_status"],
}


@dataclass(frozen=True)
class ValidationIssue:
    fecha_snapshot: str
    sheet_name: str
    severity: str
    error_code: str
    contenedor_id: str
    detail: str


def standardize_and_validate(
    sheets: dict[str, pd.DataFrame],
    snapshot_date: str,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    standardized: dict[str, pd.DataFrame] = {}
    issues: list[ValidationIssue] = []

    for sheet_name in SHEET_ORDER:
        raw_df = sheets.get(sheet_name)
        if raw_df is None:
            if sheet_name in REQUIRED_COLUMNS:
                issues.append(
                    ValidationIssue(
                        fecha_snapshot=snapshot_date,
                        sheet_name=sheet_name,
                        severity="CRITICAL",
                        error_code="missing_sheet",
                        contenedor_id="",
                        detail=f"No se encontro la hoja requerida {sheet_name}",
                    )
                )
            standardized[sheet_name] = _empty_sheet(sheet_name)
            continue

        df = _standardize_columns(raw_df, sheet_name)
        df = _coerce_types(df, sheet_name)
        standardized[sheet_name] = df
        issues.extend(_validate_required_columns(df, sheet_name, snapshot_date))
        issues.extend(_validate_sheet_rules(df, sheet_name, snapshot_date))

    return standardized, issues_to_dataframe(issues)


def issues_to_dataframe(issues: list[ValidationIssue]) -> pd.DataFrame:
    if not issues:
        return pd.DataFrame(
            columns=[
                "fecha_snapshot",
                "sheet_name",
                "severity",
                "error_code",
                "contenedor_id",
                "detail",
            ]
        )
    return pd.DataFrame([issue.__dict__ for issue in issues])


def _standardize_columns(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    alias_map = ALIASES_BY_SHEET.get(sheet_name, {})
    renamed_columns: dict[str, str] = {}
    used_targets: set[str] = set()

    for column in df.columns:
        normalized = normalize_name(column)
        target = alias_map.get(normalized, normalized)
        if target in used_targets:
            continue
        renamed_columns[column] = target
        used_targets.add(target)

    standardized = df.rename(columns=renamed_columns).copy()
    standardized.columns = [normalize_name(column) for column in standardized.columns]

    for column in ALL_CANONICAL_COLUMNS.get(sheet_name, []):
        if column not in standardized.columns:
            standardized[column] = pd.NA

    standardized = standardized.loc[:, list(dict.fromkeys(standardized.columns))]
    standardized = standardized.dropna(how="all")

    if "contenedor_id" in standardized.columns:
        standardized["contenedor_id"] = standardized["contenedor_id"].astype("string").str.strip()

    return standardized


def _coerce_types(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    coerced = df.copy()
    for column in DATE_COLUMNS.get(sheet_name, []):
        if column in coerced.columns:
            coerced[column] = coerced[column].apply(coerce_excel_date)

    for column in TEXT_COLUMNS.get(sheet_name, []):
        if column in coerced.columns:
            coerced[column] = coerced[column].astype("string").str.strip()

    if sheet_name == "Status_Operativo" and "horario_entrega_real" in coerced.columns:
        coerced["horario_entrega_real"] = coerced["horario_entrega_real"].astype("string").str.strip()

    if sheet_name == "Planif_Grupasa" and "hora_descarga" in coerced.columns:
        coerced["hora_descarga"] = coerced["hora_descarga"].astype("string").str.strip()

    return coerced


def _validate_required_columns(
    df: pd.DataFrame,
    sheet_name: str,
    snapshot_date: str,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    required_columns = REQUIRED_COLUMNS.get(sheet_name, [])
    missing = [column for column in required_columns if column not in df.columns]
    for column in missing:
        issues.append(
            ValidationIssue(
                fecha_snapshot=snapshot_date,
                sheet_name=sheet_name,
                severity="CRITICAL",
                error_code="missing_column",
                contenedor_id="",
                detail=f"Falta la columna requerida {column}",
            )
        )
    return issues


def _validate_sheet_rules(
    df: pd.DataFrame,
    sheet_name: str,
    snapshot_date: str,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if df.empty:
        return issues
    if sheet_name == "Control_Calidad":
        return issues

    if "contenedor_id" in df.columns:
        duplicate_mask = df["contenedor_id"].fillna("").duplicated(keep=False) & df["contenedor_id"].notna()
        for container_id in df.loc[duplicate_mask, "contenedor_id"].astype(str).unique():
            issues.append(
                ValidationIssue(
                    fecha_snapshot=snapshot_date,
                    sheet_name=sheet_name,
                    severity="ERROR",
                    error_code="duplicate_contenedor_id",
                    contenedor_id=container_id,
                    detail="El contenedor aparece mas de una vez en la hoja",
                )
            )

    if sheet_name == "Registro_Contenedores":
        cas_empty = df["fecha_cas"].isna()
        for container_id in df.loc[cas_empty, "contenedor_id"].fillna("").astype(str):
            issues.append(
                ValidationIssue(
                    fecha_snapshot=snapshot_date,
                    sheet_name=sheet_name,
                    severity="ERROR",
                    error_code="fecha_cas_vacia",
                    contenedor_id=container_id,
                    detail="La fecha CAS esta vacia",
                )
            )

    if sheet_name == "Status_Operativo":
        normalized_status = df["status_actual"].fillna("").astype(str).str.strip().str.upper()
        empty_status = normalized_status.eq("")
        for container_id in df.loc[empty_status, "contenedor_id"].fillna("").astype(str):
            issues.append(
                ValidationIssue(
                    fecha_snapshot=snapshot_date,
                    sheet_name=sheet_name,
                    severity="ERROR",
                    error_code="status_vacio",
                    contenedor_id=container_id,
                    detail="El status actual esta vacio",
                )
            )

    return issues


def _empty_sheet(sheet_name: str) -> pd.DataFrame:
    return pd.DataFrame(columns=ALL_CANONICAL_COLUMNS.get(sheet_name, []))


def normalize_name(value: object) -> str:
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.replace("/", "_").replace("-", "_").replace(" ", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def coerce_excel_date(value: object) -> object:
    if pd.isna(value) or value == "":
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value.date()
    if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day") and not isinstance(value, str):
        return value

    if isinstance(value, (int, float)) and not pd.isna(value):
        if 20000 <= float(value) <= 60000:
            converted = pd.to_datetime("1899-12-30") + pd.to_timedelta(float(value), unit="D")
            return converted.date()

    text = str(value).strip()
    converted = pd.to_datetime(text, errors="coerce")
    if not pd.isna(converted):
        return converted.date()
    return pd.NaT
