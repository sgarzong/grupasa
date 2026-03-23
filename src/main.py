from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from src.config import ensure_directories, get_settings
from src.download_source import fetch_source_workbook, read_source_sheets
from src.export_outputs import export_csv
from src.snapshot import append_daily_snapshot
from src.transform import CURRENT_OUTPUT_COLUMNS, build_current_dataset
from src.validate import standardize_and_validate


def setup_logging(log_file_path: Path) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def run_pipeline() -> int:
    settings = get_settings()
    ensure_directories(settings)
    setup_logging(settings.log_file_path)
    logger = logging.getLogger(__name__)
    logger.info("Inicio del pipeline logistico")

    pipeline_issues: list[dict[str, Any]] = []
    current_output = pd.DataFrame(columns=CURRENT_OUTPUT_COLUMNS)

    try:
        download_result = fetch_source_workbook(settings)
        for warning in download_result.warnings:
            pipeline_issues.append(_build_pipeline_issue(settings.snapshot_date, "WARNING", "download_warning", warning))

        if download_result.source_path is None:
            raise RuntimeError("No existe fuente descargada ni cacheada para procesar")

        raw_sheets = read_source_sheets(download_result.source_path)
        standardized_sheets, validation_errors = standardize_and_validate(raw_sheets, settings.snapshot_date)

        status_history = append_daily_snapshot(
            standardized_sheets["Status_Operativo"],
            settings.status_historico_path,
            settings.snapshot_date,
            ["contenedor_id"],
        )
        append_daily_snapshot(
            standardized_sheets["Registro_Contenedores"],
            settings.registro_congelado_path,
            settings.snapshot_date,
            ["contenedor_id"],
        )
        append_daily_snapshot(
            standardized_sheets["Planif_Galagans"],
            settings.plan_galagans_congelado_path,
            settings.snapshot_date,
            ["contenedor_id"],
        )

        current_output = build_current_dataset(
            standardized_sheets,
            status_history=status_history,
            snapshot_date=settings.snapshot_date,
            cas_alert_days=settings.cas_alert_days,
        )

        all_issues = pd.concat([validation_errors, pd.DataFrame(pipeline_issues)], ignore_index=True, sort=False)
        export_csv(current_output, settings.contenedores_actual_path)
        export_csv(all_issues, settings.errores_validacion_path)
        logger.info("Pipeline completado. Filas actuales=%s | errores=%s", len(current_output), len(all_issues))
        return 0
    except Exception as exc:
        logger.exception("Fallo controlado en pipeline: %s", exc)
        pipeline_issues.append(_build_pipeline_issue(settings.snapshot_date, "CRITICAL", "pipeline_failure", str(exc)))
        export_csv(current_output, settings.contenedores_actual_path)
        export_csv(pd.DataFrame(pipeline_issues), settings.errores_validacion_path)
        return 1


def _build_pipeline_issue(
    snapshot_date: str,
    severity: str,
    error_code: str,
    detail: str,
) -> dict[str, str]:
    return {
        "fecha_snapshot": snapshot_date,
        "sheet_name": "pipeline",
        "severity": severity,
        "error_code": error_code,
        "contenedor_id": "",
        "detail": detail,
    }


if __name__ == "__main__":
    raise SystemExit(run_pipeline())
