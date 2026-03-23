# Proyecto Logistico Sheets -> CSV para Power BI

Pipeline cloud-first y de costo cero para descargar diariamente un Google Sheet operativo, mantener historicos operativos y publicar una capa CSV en modelo estrella lista para Power BI.

## Arquitectura

Entrada:

- Google Sheet exportable a XLSX
- Hojas requeridas: `Registro_Contenedores`, `Planif_Grupasa`, `Planif_Galagans`, `Status_Operativo`
- Hoja opcional: `Control_Calidad`

Proceso:

1. `download_source.py` descarga el XLSX desde `SOURCE_XLSX_URL` o usa `SOURCE_LOCAL_PATH`.
2. `validate.py` estandariza nombres de columnas, tipa fechas y registra errores de estructura y reglas de negocio.
3. `snapshot.py` persiste:
   - historico diario solo para `Status_Operativo`
   - estado vigente sin duplicados por contenedor para `Registro_Contenedores`
   - estado vigente sin duplicados por contenedor para `Planif_Galagans`
4. `transform.py` consolida una fila actual por contenedor y deriva alertas, cumplimiento y tiempos entre etapas usando snapshots historicos.
5. `transform.py` tambien construye una capa estrella para BI con dimensiones y hechos separados.
6. `export_outputs.py` publica los CSV finales para operacion y Power BI.
7. GitHub Actions ejecuta el flujo diariamente y versiona los historicos dentro del repositorio.

Salida:

- `data/curated/contenedores_actual.csv`
- `data/history/status_historico.csv`
- `data/history/registro_congelado.csv`
- `data/history/plan_galagans_congelado.csv`
- `data/quality/errores_validacion.csv`
- `data/powerbi/dim_contenedor.csv`
- `data/powerbi/dim_fecha.csv`
- `data/powerbi/dim_status.csv`
- `data/powerbi/dim_bodega.csv`
- `data/powerbi/fact_status_diario.csv`
- `data/powerbi/fact_plan_actual.csv`
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
    protect_sheet.py
    sample_data.py
    snapshot.py
    transform.py
    validate.py
  data/
    raw/
    curated/
    history/
    quality/
    powerbi/
  logs/
  .github/
    workflows/
      pipeline.yml
  tests/
    test_validate.py
    test_transform.py
```

## Clave de integracion

La clave de union entre hojas es `contenedor_id`.

Diseno adoptado:

- `Registro_Contenedores` es la fuente maestra preferida.
- Si un contenedor existe solo en planificacion o status, igual entra al consolidado actual.
- En la estandarizacion se aceptan aliases comunes como `Contenedor`, `contenedor_id`, `container`.

## Reglas de negocio implementadas

- `alerta_cas = true` si `status_actual == "EN PUERTO"` y faltan entre 0 y `CAS_ALERT_DAYS` dias para `fecha_cas`.
- `cas_vencido = true` si `status_actual == "EN PUERTO"` y `fecha_cas < fecha_snapshot`.
- `cumplimiento_grupasa`:
  - compara `plan_llegada_grupasa` contra la primera fecha historica en que el contenedor llego a etapa `BODEGA`
  - valores posibles: `CUMPLE`, `INCUMPLE`, `PENDIENTE`, `SIN_PLAN`
- `cumplimiento_galagans`:
  - prioridad 1: compara `plan_devolucion_vacio` contra la primera fecha historica en que el contenedor llego a `DEPOSITO`
  - prioridad 2: si no existe plan de devolucion, compara `plan_llegada_patio` contra la primera fecha historica en `PATIO`
  - valores posibles: `CUMPLE`, `INCUMPLE`, `PENDIENTE`, `SIN_PLAN`
- Duraciones:
  - `dias_puerto_a_patio`
  - `dias_patio_a_bodega`
  - `dias_bodega_a_deposito`

## Inferencias documentadas

Como no hay fechas reales explicitas para todos los hitos, las fechas operativas se infieren a partir de snapshots diarios:

- primera fecha en `PUERTO`: primer snapshot con status que contenga `PUERTO`; si no existe, usa `fecha_arribo`
- primera fecha en `PATIO`: primer snapshot cuyo status contenga `PATIO`
- primera fecha en `BODEGA`: primer snapshot cuyo status contenga `BODEGA` o `ENTREGADO`
- primera fecha en `DEPOSITO`: primer snapshot cuyo status contenga `DEPOSITO` o `VACIO`

## Semantica de persistencia

- `status_historico.csv`:
  - guarda una fila por `contenedor_id + fecha_snapshot`
  - si el pipeline corre mas de una vez el mismo dia, conserva solo la ultima corrida de ese dia
- `registro_congelado.csv`:
  - no acumula snapshots diarios
  - conserva solo el ultimo estado conocido por `contenedor_id`
- `plan_galagans_congelado.csv`:
  - no acumula snapshots diarios
  - conserva solo el ultimo estado conocido por `contenedor_id`

## Validaciones implementadas

El modulo `validate.py` revisa:

- columnas faltantes
- hojas faltantes
- contenedores duplicados
- `fecha_cas` vacia
- `status_actual` vacio
- `Horario_Entrega_Real` lleno cuando `status_actual != ENTREGADO`
- `status_actual == ENTREGADO` sin `Horario_Entrega_Real`

El pipeline no se rompe por errores de calidad. Registra incidencias en `data/quality/errores_validacion.csv` y sigue generando salidas con la mejor informacion disponible.

## Esquema de salidas operativas

### `contenedores_actual.csv`

Columnas minimas:

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

## Modelo estrella para Power BI

La capa recomendada para BI es `data/powerbi/`.

Tablas:

- `dim_contenedor.csv`: atributos maestros del contenedor
- `dim_fecha.csv`: calendario para todas las fechas relevantes
- `dim_status.csv`: catalogo de status y etapa derivada
- `dim_bodega.csv`: catalogo de bodegas
- `fact_status_diario.csv`: grano `1 fila por contenedor por fecha_snapshot`
- `fact_plan_actual.csv`: grano `1 fila por contenedor en la foto vigente`

Relaciones sugeridas en Power BI:

- `dim_contenedor[contenedor_key]` -> `fact_status_diario[contenedor_key]`
- `dim_contenedor[contenedor_key]` -> `fact_plan_actual[contenedor_key]`
- `dim_status[status_key]` -> `fact_status_diario[status_key]`
- `dim_status[status_key]` -> `fact_plan_actual[status_key]`
- `dim_bodega[bodega_key]` -> `fact_status_diario[bodega_key]`
- `dim_bodega[bodega_key]` -> `fact_plan_actual[bodega_key]`
- `dim_fecha[fecha_key]` -> `fact_status_diario[fecha_key]`
- `dim_fecha[fecha_key]` -> `fact_plan_actual[snapshot_fecha_key]`

Relaciones de fecha adicionales recomendadas como inactivas:

- `dim_fecha[fecha_key]` -> `fact_plan_actual[fecha_arribo_key]`
- `dim_fecha[fecha_key]` -> `fact_plan_actual[fecha_cas_key]`
- `dim_fecha[fecha_key]` -> `fact_plan_actual[plan_llegada_grupasa_key]`
- `dim_fecha[fecha_key]` -> `fact_plan_actual[plan_llegada_patio_key]`
- `dim_fecha[fecha_key]` -> `fact_plan_actual[plan_devolucion_vacio_key]`

## Power BI desde GitHub

Si no cuentas con OneDrive ni SharePoint, Power BI debe consumir estos CSV usando el conector `Web` apuntando a las URLs `raw` de GitHub.

Repositorio actual:

- `https://github.com/sgarzong/grupasa`

URLs raw sugeridas:

- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_contenedor.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_fecha.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_status.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_bodega.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/fact_status_diario.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/fact_plan_actual.csv`

Recomendaciones:

- usa `Obtener datos` -> `Web`
- pega cada URL raw exacta
- no uses rutas locales ni la pagina HTML de GitHub
- no cambies nombres ni ubicaciones de estos CSV si ya montaste el modelo en Power BI
- el refresh en Power BI Service sera de tipo import, no DirectQuery

## Bloqueo en Google Sheets

El proyecto ya soporta proteccion automatica en Google Sheets, pero requiere configuracion adicional.

Que hace:

- despues de una corrida exitosa
- elimina protecciones previas administradas por el pipeline
- vuelve a crear protecciones sobre las filas de datos de:
  - `Registro_Contenedores`
  - `Planif_Grupasa`
  - `Planif_Galagans`

Suposicion aplicada:

- se bloquea la fila completa de datos cargada en cada una de esas hojas
- `Status_Operativo` no se bloquea

Que necesitas configurar:

1. Crear una service account en Google Cloud.
2. Habilitar Google Sheets API en ese proyecto.
3. Habilitar Google Drive API en ese proyecto.
4. Compartir el Google Sheet con el email de la service account como `Editor`.
5. Guardar el JSON completo de la credencial en GitHub Secret:
   - `GOOGLE_SERVICE_ACCOUNT_JSON`
6. Crear la variable de repositorio:
   - `GOOGLE_SHEETS_ENABLE_PROTECTION=true`

Notas:

- sin estas credenciales, el pipeline sigue funcionando en modo solo lectura
- la fuente sigue siendo `SOURCE_XLSX_URL`, pero la proteccion usa el `spreadsheetId` extraido de esa URL

## Conversion a Google Sheet nativo

Si el archivo fuente realmente es un documento Office almacenado en Drive y no un Google Sheet nativo, la proteccion de rangos no funcionara sobre ese archivo.

Para ayudarte a migrarlo, existe el script:

```powershell
python .\scripts\convert_source_to_native_sheet.py --service-account-json "<ruta-json>" --source-url "<url-export-xlsx>"
```

Ese script:

- inspecciona el archivo fuente en Drive
- si ya es nativo, te devuelve su URL correcta
- si no es nativo, crea una copia Google Sheets y te devuelve la nueva URL exportable

Despues de eso debes actualizar `SOURCE_XLSX_URL` para apuntar al nuevo Google Sheet nativo.

## Configuracion

Copiar `.env.example` a `.env` si quieres parametrizar localmente.

Variables:

- `SOURCE_XLSX_URL`: URL exportable del Google Sheet
- `SOURCE_LOCAL_PATH`: ruta local a un XLSX para pruebas o contingencia
- `CAS_ALERT_DAYS`: ventana de alerta CAS
- `TIMEZONE`: `America/Guayaquil`
- `GOOGLE_SHEETS_ENABLE_PROTECTION`: `true/false` para activar bloqueo post-pipeline
- `GOOGLE_SERVICE_ACCOUNT_JSON`: JSON completo de la cuenta de servicio con acceso editor al Google Sheet

## Como cambiar la URL fuente

Editar `SOURCE_XLSX_URL` en `.env` o en las variables del repositorio en GitHub.

Ejemplo:

```env
SOURCE_XLSX_URL=https://docs.google.com/spreadsheets/d/<nuevo-id>/export?format=xlsx
```

## Ejecucion local

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

## GitHub Actions

El workflow corre cada dia a las `15:00 UTC`, que equivale a `10:00 AM` hora Ecuador, y tambien soporta `workflow_dispatch`.
