from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.export_outputs import export_csv


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    current_path = project_root / "data" / "curated" / "contenedores_actual.csv"
    output_path = project_root / "data" / "quality" / "inventario_historico_inconsistencias.csv"

    current = pd.read_csv(current_path)
    for column in [
        "fecha_arribo_gye",
        "fecha_salida_autorizada",
        "fecha_arribo",
        "fecha_cas",
        "plan_llegada_grupasa",
        "plan_devolucion_vacio",
        "fecha_snapshot",
    ]:
        if column in current.columns:
            current[column] = pd.to_datetime(current[column], errors="coerce")

    issue_frames: list[pd.DataFrame] = []

    checks = {
        "cas_antes_salida_autorizada": current["fecha_cas"] < current["fecha_salida_autorizada"],
        "plan_grupasa_antes_retiro_puerto": current["plan_llegada_grupasa"] < current["fecha_arribo"],
        "plan_devolucion_antes_retiro_puerto": current["plan_devolucion_vacio"] < current["fecha_arribo"],
        "retiro_puerto_antes_arribo_gye": current["fecha_arribo"] < current["fecha_arribo_gye"],
        "cas_antes_arribo_gye": current["fecha_cas"] < current["fecha_arribo_gye"],
        "plan_grupasa_antes_arribo_gye": current["plan_llegada_grupasa"] < current["fecha_arribo_gye"],
        "devolucion_antes_arribo_gye": current["plan_devolucion_vacio"] < current["fecha_arribo_gye"],
    }

    for check_name, mask in checks.items():
        bad = current.loc[mask].copy()
        if bad.empty:
            continue
        bad.insert(0, "issue_type", check_name)
        issue_frames.append(bad)

    missing_checks = {
        "sin_fecha_arribo_gye": current["fecha_arribo_gye"].isna(),
        "sin_plan_llegada_grupasa": current["plan_llegada_grupasa"].isna(),
        "sin_plan_devolucion_vacio": current["plan_devolucion_vacio"].isna(),
    }

    for check_name, mask in missing_checks.items():
        bad = current.loc[mask].copy()
        if bad.empty:
            continue
        bad.insert(0, "issue_type", check_name)
        issue_frames.append(bad)

    output = pd.concat(issue_frames, ignore_index=True, sort=False) if issue_frames else pd.DataFrame(columns=["issue_type", *current.columns.tolist()])
    export_csv(output, output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
