from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.download_source import _detect_header_row


def test_detect_header_row_skips_intro_rows() -> None:
    preview = pd.DataFrame(
        [
            ["1. REGISTRO DE CONTENEDORES", None, None],
            [None, None, None],
            ["ID_Contenedor", "Pedido", "Fecha_CAS"],
            ["MSCU4510021", "PED-260401", "2026-04-15"],
        ]
    )

    assert _detect_header_row(preview, "Registro_Contenedores") == 2
