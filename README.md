# Proyecto Logístico Sheets -> CSV para Power BI

Pipeline cloud-first y de costo cero para descargar diariamente un Google Sheet operativo, congelar snapshots append-only y publicar archivos CSV curados listos para Power BI.

## Arquitectura

Entrada:

- Google Sheet exportable a XLSX
- Hojas requeridas: `Registro_Contenedores`, `Planif_Grupasa`, `Planif_Galagans`, `Status_Operativo`
- Hoja opcional: `Control_Calidad`

Proceso:

1. `download_source.py` descarga el XLSX desde `SOURCE_XLSX_URL` o usa `SOURCE_LOCAL_PATH`.
2. `validate.py` estandariza nombres de columnas, tipa fechas y registra errores de estructura y reglas de negocio.
3. `snapshot.py` persiste snapshots diarios append-only en CSV para `Registro_Contenedores`, `Planif_Galagans` y `Status_Operativo`.
4. `transform.py` consolida una fila actual por contenedor y deriva alertas, cumplimiento y tiempos entre etapas usando snapshots históricos.
5. `export_outputs.py` publica los CSV finales para Power BI.
6. GitHub Actions ejecuta el flujo diariamente y versiona los históricos dentro del repositorio.

Salida:

- `data/curated/contenedores_actual.csv`
- `data/history/status_historico.csv`
- `data/history/registro_congelado.csv`
- `data/history/plan_galagans_congelado.csv`
- `data/quality/errores_validacion.csv`
- `logs/pipeline.log`

## Estructura

```text
project/
  README.md
  requirements.txt
  .gitignore
  .env.example
  src/
    config.py
    download_source.py
    export_outputs.py
    main.py
    sample_data.py
    snapshot.py
    transform.py
    validate.py
  data/
    raw/
    curated/
    history/
    quality/
  logs/
  .github/
    workflows/
      pipeline.yml
  tests/
    test_validate.py
    test_transform.py
```

## Clave de integración

La clave de unión entre hojas es `contenedor_id`.

Diseño adoptado:

- `Registro_Contenedores` es la fuente maestra preferida.
- Si un contenedor existe solo en planificación o status, igual entra al consolidado actual.
- En la estandarización se aceptan aliases comunes como `Contenedor`, `contenedor_id`, `container`.

## Reglas de negocio implementadas

- `alerta_cas = true` si `status_actual == "EN PUERTO"` y faltan entre 0 y `CAS_ALERT_DAYS` días para `fecha_cas`.
- `cas_vencido = true` si `status_actual == "EN PUERTO"` y `fecha_cas < fecha_snapshot`.
- `cumplimiento_grupasa`:
  - compara `plan_llegada_grupasa` contra la primera fecha histórica en que el contenedor llegó a etapa `BODEGA`
  - valores posibles: `CUMPLE`, `INCUMPLE`, `PENDIENTE`, `SIN_PLAN`
- `cumplimiento_galagans`:
  - prioridad 1: compara `plan_devolucion_vacio` contra la primera fecha histórica en que el contenedor llegó a `DEPOSITO`
  - prioridad 2: si no existe plan de devolución, compara `plan_llegada_patio` contra la primera fecha histórica en `PATIO`
  - valores posibles: `CUMPLE`, `INCUMPLE`, `PENDIENTE`, `SIN_PLAN`
- Duraciones:
  - `dias_puerto_a_patio`
  - `dias_patio_a_bodega`
  - `dias_bodega_a_deposito`

## Inferencias documentadas

Como no hay fechas reales explícitas para todos los hitos, las fechas operativas se infieren a partir de snapshots diarios:

- primera fecha en `PUERTO`: primer snapshot con status que contenga `PUERTO`; si no existe, usa `fecha_arribo`
- primera fecha en `PATIO`: primer snapshot cuyo status contenga `PATIO`
- primera fecha en `BODEGA`: primer snapshot cuyo status contenga `BODEGA` o `ENTREGADO`
- primera fecha en `DEPOSITO`: primer snapshot cuyo status contenga `DEPOSITO` o `VACIO`

## Validaciones implementadas

El módulo `validate.py` revisa:

- columnas faltantes
- hojas faltantes
- contenedores duplicados
- `fecha_cas` vacía
- `status_actual` vacío
- `Horario_Entrega_Real` lleno cuando `status_actual != ENTREGADO`
- `status_actual == ENTREGADO` sin `Horario_Entrega_Real`

El pipeline no se rompe por errores de calidad. Registra incidencias en `data/quality/errores_validacion.csv` y sigue generando salidas con la mejor información disponible.

## Esquema de salidas

### `contenedores_actual.csv`

Columnas mínimas:

- `fecha_snapshot`
- `contenedor_id`
- `pedido`
- `parcial`
- `naviera`
- `puerto`
- `deposito_vacio`
- `fecha_arribo`
- `fecha_cas`
- `plan_llegada_grupasa`
- `bodega`
- `hora_descarga`
- `comentario_plan_grupasa`
- `plan_llegada_patio`
- `plan_devolucion_vacio`
- `comentario_plan_galagans`
- `status_actual`
- `horario_entrega_real`
- `tipo_incidencia`
- `comentario_status`
- `alerta_cas`
- `cas_vencido`
- `dias_puerto_a_patio`
- `dias_patio_a_bodega`
- `dias_bodega_a_deposito`
- `cumplimiento_grupasa`
- `cumplimiento_galagans`

### `errores_validacion.csv`

Columnas:

- `fecha_snapshot`
- `sheet_name`
- `severity`
- `error_code`
- `contenedor_id`
- `detail`

Los históricos son append-only por `fecha_snapshot + contenedor_id`. Si el workflow corre dos veces el mismo día, reemplaza solo la foto de ese día y preserva el pasado.

## Configuración

Copiar `.env.example` a `.env` si quieres parametrizar localmente.

Variables:

- `SOURCE_XLSX_URL`: URL exportable del Google Sheet
- `SOURCE_LOCAL_PATH`: ruta local a un XLSX para pruebas o contingencia
- `CAS_ALERT_DAYS`: ventana de alerta CAS
- `TIMEZONE`: `America/Guayaquil`

## Cómo cambiar la URL fuente

Editar `SOURCE_XLSX_URL` en `.env` o en las variables del repositorio en GitHub.

Ejemplo:

```env
SOURCE_XLSX_URL=https://docs.google.com/spreadsheets/d/<nuevo-id>/export?format=xlsx
```

## Ejecución local

```powershell
cd project
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.sample_data
$env:SOURCE_LOCAL_PATH = (Resolve-Path .\data\raw\sample_logistica.xlsx)
python -m src.main
pytest
```

## Power BI

- `contenedores_actual.csv` es la tabla principal actual.
- `status_historico.csv` soporta análisis temporal y permanencia por etapa.
- `registro_congelado.csv` y `plan_galagans_congelado.csv` permiten auditoría de cambios diarios.
- `errores_validacion.csv` puede mostrarse como tablero de calidad de datos.

## GitHub Actions

El workflow corre cada día a las `15:00 UTC`, que equivale a `10:00 AM` hora Ecuador, y también soporta `workflow_dispatch`.
