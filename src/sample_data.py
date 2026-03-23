from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import BASE_DIR


def build_sample_workbook(output_path: Path | None = None) -> Path:
    output = output_path or (BASE_DIR / "data" / "raw" / "sample_logistica.xlsx")
    output.parent.mkdir(parents=True, exist_ok=True)

    registro = pd.DataFrame(
        [
            {
                "Contenedor": "MSCU1234567",
                "Pedido": "PED-001",
                "Parcial": "P1",
                "Naviera": "MSC",
                "Puerto": "Guayaquil",
                "Deposito_Vacio": "Depo Norte",
                "Fecha_Arribo": "2026-03-18",
                "Fecha_CAS": "2026-03-24",
            },
            {
                "Contenedor": "TGHU7654321",
                "Pedido": "PED-002",
                "Parcial": "P2",
                "Naviera": "MAERSK",
                "Puerto": "Posorja",
                "Deposito_Vacio": "Depo Sur",
                "Fecha_Arribo": "2026-03-16",
                "Fecha_CAS": "2026-03-20",
            },
            {
                "Contenedor": "OOLU1111111",
                "Pedido": "PED-003",
                "Parcial": "P1",
                "Naviera": "OOCL",
                "Puerto": "Guayaquil",
                "Deposito_Vacio": "Depo Norte",
                "Fecha_Arribo": "2026-03-14",
                "Fecha_CAS": "2026-03-21",
            },
        ]
    )

    plan_grupasa = pd.DataFrame(
        [
            {
                "Contenedor": "MSCU1234567",
                "Plan_Llegada_Grupasa": "2026-03-22",
                "Bodega": "BOD-A",
                "Hora_Descarga": "08:00",
                "Comentario_Plan": "Descarga prioritaria",
            },
            {
                "Contenedor": "TGHU7654321",
                "Plan_Llegada_Grupasa": "2026-03-20",
                "Bodega": "BOD-B",
                "Hora_Descarga": "10:00",
                "Comentario_Plan": "Cliente critico",
            },
            {
                "Contenedor": "OOLU1111111",
                "Plan_Llegada_Grupasa": "2026-03-18",
                "Bodega": "BOD-C",
                "Hora_Descarga": "09:30",
                "Comentario_Plan": "Normal",
            },
        ]
    )

    plan_galagans = pd.DataFrame(
        [
            {
                "Contenedor": "MSCU1234567",
                "Plan_Llegada_Patio": "2026-03-20",
                "Plan_Devolucion_Vacio": "2026-03-28",
                "Comentario_Plan": "Patio 1",
            },
            {
                "Contenedor": "TGHU7654321",
                "Plan_Llegada_Patio": "2026-03-18",
                "Plan_Devolucion_Vacio": "2026-03-24",
                "Comentario_Plan": "Patio 2",
            },
            {
                "Contenedor": "OOLU1111111",
                "Plan_Llegada_Patio": "2026-03-15",
                "Plan_Devolucion_Vacio": "2026-03-21",
                "Comentario_Plan": "Patio 3",
            },
        ]
    )

    status = pd.DataFrame(
        [
            {
                "Contenedor": "MSCU1234567",
                "Status_Actual": "EN PUERTO",
                "Horario_Entrega_Real": "",
                "Tipo_Incidencia": "",
                "Comentario": "Pendiente retiro",
            },
            {
                "Contenedor": "TGHU7654321",
                "Status_Actual": "EN BODEGA",
                "Horario_Entrega_Real": "",
                "Tipo_Incidencia": "DEMORA",
                "Comentario": "Llego tarde al patio",
            },
            {
                "Contenedor": "OOLU1111111",
                "Status_Actual": "DEVUELTO DEPOSITO VACIO",
                "Horario_Entrega_Real": "",
                "Tipo_Incidencia": "",
                "Comentario": "Ciclo completo",
            },
        ]
    )

    control_calidad = pd.DataFrame(
        [{"Contenedor": "TGHU7654321", "Regla": "ejemplo", "Detalle": "Observacion manual"}]
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        registro.to_excel(writer, index=False, sheet_name="Registro_Contenedores")
        plan_grupasa.to_excel(writer, index=False, sheet_name="Planif_Grupasa")
        plan_galagans.to_excel(writer, index=False, sheet_name="Planif_Galagans")
        status.to_excel(writer, index=False, sheet_name="Status_Operativo")
        control_calidad.to_excel(writer, index=False, sheet_name="Control_Calidad")

    return output


if __name__ == "__main__":
    print(build_sample_workbook())
