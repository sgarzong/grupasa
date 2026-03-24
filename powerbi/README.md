# Power BI Dashboard Pack

Este paquete deja listo el modelo y el blueprint del dashboard para la capa estrella actual generada por el pipeline en `data/powerbi/`.

## Origen recomendado

Power BI debe consumir estos CSV desde GitHub con el conector `Web`:

- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_contenedor.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_fecha.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_status.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/dim_bodega.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/fact_status_diario.csv`
- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/powerbi/fact_plan_actual.csv`

Tabla opcional de calidad:

- `https://raw.githubusercontent.com/sgarzong/grupasa/main/data/quality/errores_validacion.csv`

## Que incluye

- `theme/grupasa_theme.json`: tema visual para el reporte
- `model/semantic_model.md`: relaciones y uso del modelo estrella
- `model/measures.dax`: medidas base
- `report/dashboard_spec.md`: blueprint de paginas y visuales
- `mcp/prompts.md`: prompts para Power BI Modeling MCP

## Tablas recomendadas en Power BI

- `DimContenedor`
- `DimFecha`
- `DimStatus`
- `DimBodega`
- `FactStatusDiario`
- `FactPlanActual`
- `FactErroresValidacion` opcional

## Campos nuevos relevantes

La version actual del pipeline ya expone en `FactPlanActual`:

- `fecha_arribo_gye`
- `fecha_salida_autorizada`
- `plan_slot_grupasa`
- `tipo_asignacion_grupasa`

Interpretacion:

- `plan_slot_grupasa`: orden del slot consumido dentro del pedido
- `tipo_asignacion_grupasa`:
  - `persistida`: asignacion ya consolidada por movimiento real
  - `directa_hoja`: el contenedor aun conserva la planificacion directa de la hoja porque no ha requerido reasignacion

## Flujo recomendado en Power BI Desktop

1. `Obtener datos` -> `Web`
2. Cargar cada URL raw
3. Renombrar tablas:
   - `dim_contenedor` -> `DimContenedor`
   - `dim_fecha` -> `DimFecha`
   - `dim_status` -> `DimStatus`
   - `dim_bodega` -> `DimBodega`
   - `fact_status_diario` -> `FactStatusDiario`
   - `fact_plan_actual` -> `FactPlanActual`
   - `errores_validacion` -> `FactErroresValidacion`
4. Crear relaciones segun `model/semantic_model.md`
5. Cargar medidas de `model/measures.dax`
6. Aplicar `theme/grupasa_theme.json`
7. Construir paginas segun `report/dashboard_spec.md`

## Reglas practicas

- usa siempre `raw.githubusercontent.com`, no la pagina HTML del repo
- no cambies nombres ni rutas de los CSV si ya conectaste el modelo
- el refresh en Power BI Service sera de tipo import
