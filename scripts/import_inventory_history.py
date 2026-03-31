from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import ensure_directories, get_settings
from src.export_outputs import export_csv
from src.inventory_history import build_historical_outputs, load_inventory_workbook


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa inventarios historicos y genera snapshots diarios para Power BI.")
    parser.add_argument("files", nargs="+", help="Rutas a los archivos Excel historicos.")
    args = parser.parse_args()

    settings = get_settings()
    ensure_directories(settings)

    workbooks = [load_inventory_workbook(Path(file_path)) for file_path in args.files]
    outputs = build_historical_outputs(workbooks, cas_alert_days=settings.cas_alert_days)

    export_csv(outputs["status_historico"], settings.status_historico_path)
    export_csv(outputs["registro_congelado"], settings.registro_congelado_path)
    export_csv(outputs["plan_galagans_congelado"], settings.plan_galagans_congelado_path)
    export_csv(outputs["contenedores_actual"], settings.contenedores_actual_path)
    export_csv(pd.DataFrame(columns=["fecha_snapshot", "sheet_name", "severity", "error_code", "contenedor_id", "detail"]), settings.errores_validacion_path)
    export_csv(pd.DataFrame(columns=["fecha_snapshot", "pedido", "plan_slot", "contenedor_id", "fecha_primer_movimiento", "plan_llegada_grupasa", "bodega", "hora_descarga", "comentario_plan_grupasa", "tipo_asignacion"]), settings.asignacion_plan_grupasa_path)
    export_csv(outputs["dim_contenedor"], settings.dim_contenedor_path)
    export_csv(outputs["dim_fecha"], settings.dim_fecha_path)
    export_csv(outputs["dim_status"], settings.dim_status_path)
    export_csv(outputs["dim_bodega"], settings.dim_bodega_path)
    export_csv(outputs["fact_status_diario"], settings.fact_status_diario_path)
    export_csv(outputs["fact_plan_actual"], settings.fact_plan_actual_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
