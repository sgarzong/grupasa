from __future__ import annotations

from pathlib import Path
import shutil
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.snapshot import append_daily_snapshot, upsert_latest_snapshot


def test_append_daily_snapshot_keeps_only_last_run_for_same_day() -> None:
    temp_dir = Path(__file__).resolve().parents[1] / "tests_runtime"
    temp_dir.mkdir(exist_ok=True)
    target = temp_dir / "status_historico.csv"
    if target.exists():
        target.unlink()

    first = pd.DataFrame([{"contenedor_id": "C1", "status_actual": "EN PUERTO"}])
    second = pd.DataFrame([{"contenedor_id": "C1", "status_actual": "ENTREGADO"}])

    append_daily_snapshot(first, target, "2026-03-23", ["contenedor_id"])
    result = append_daily_snapshot(second, target, "2026-03-23", ["contenedor_id"])

    assert len(result) == 1
    assert result.iloc[0]["status_actual"] == "ENTREGADO"
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_upsert_latest_snapshot_does_not_repeat_same_container_across_days() -> None:
    temp_dir = Path(__file__).resolve().parents[1] / "tests_runtime"
    temp_dir.mkdir(exist_ok=True)
    target = temp_dir / "registro_congelado.csv"
    if target.exists():
        target.unlink()

    day_one = pd.DataFrame([{"contenedor_id": "C1", "pedido": "PED-1"}])
    day_two = pd.DataFrame([{"contenedor_id": "C1", "pedido": "PED-1B"}])

    upsert_latest_snapshot(day_one, target, "2026-03-22", ["contenedor_id"])
    result = upsert_latest_snapshot(day_two, target, "2026-03-23", ["contenedor_id"])

    assert len(result) == 1
    assert result.iloc[0]["pedido"] == "PED-1B"
    assert result.iloc[0]["fecha_snapshot"] == "2026-03-23"
    shutil.rmtree(temp_dir, ignore_errors=True)
