# Power BI Dashboard Pack

Este paquete deja listo el diseno funcional del dashboard para la capa estrella generada por el pipeline en `data/powerbi/`.

Tablas fuente recomendadas:

- `data/powerbi/dim_contenedor.csv`
- `data/powerbi/dim_fecha.csv`
- `data/powerbi/dim_status.csv`
- `data/powerbi/dim_bodega.csv`
- `data/powerbi/fact_status_diario.csv`
- `data/powerbi/fact_plan_actual.csv`

Tablas de apoyo opcionales:

- `data/quality/errores_validacion.csv`
- `data/curated/contenedores_actual.csv`

## Que incluye

- `theme/grupasa_theme.json`: tema visual para el reporte.
- `model/semantic_model.md`: modelo recomendado y relaciones.
- `model/measures.dax`: medidas DAX base para KPIs, cumplimiento y calidad.
- `report/dashboard_spec.md`: blueprint de paginas y visuales.
- `mcp/prompts.md`: prompts listos para usar con Power BI Modeling MCP.

## Tablas recomendadas en Power BI

- `DimContenedor`
- `DimFecha`
- `DimStatus`
- `DimBodega`
- `FactStatusDiario`
- `FactPlanActual`
- `FactErroresValidacion` opcional

## Flujo recomendado en Power BI Desktop

1. Importa los CSV de `data/powerbi/` usando `Obtener datos` -> `Web` si vas a consumir desde GitHub.
2. Renombra tablas segun la convencion anterior.
3. Relaciona:
   - `DimContenedor[contenedor_key]` -> `FactStatusDiario[contenedor_key]`
   - `DimContenedor[contenedor_key]` -> `FactPlanActual[contenedor_key]`
   - `DimStatus[status_key]` -> `FactStatusDiario[status_key]`
   - `DimStatus[status_key]` -> `FactPlanActual[status_key]`
   - `DimBodega[bodega_key]` -> `FactStatusDiario[bodega_key]`
   - `DimBodega[bodega_key]` -> `FactPlanActual[bodega_key]`
   - `DimFecha[fecha_key]` -> `FactStatusDiario[fecha_key]`
   - `DimFecha[fecha_key]` -> `FactPlanActual[snapshot_fecha_key]`
4. Crea relaciones de fecha inactivas adicionales desde `DimFecha` hacia los otros `*_key` de `FactPlanActual`.
5. Carga las medidas de `model/measures.dax`.
6. Aplica `theme/grupasa_theme.json`.
7. Construye las paginas segun `report/dashboard_spec.md`.

## Sobre GitHub como origen

Si Power BI Service va a refrescar desde GitHub:

- usa `raw.githubusercontent.com`
- no uses la pagina HTML del repo
- manten fijas las rutas y nombres de archivo
- conecta cada CSV con el conector `Web`

## Sobre MCP

El Modeling MCP te sirve muy bien para:

- crear medidas
- ordenar campos
- definir relaciones
- documentar el modelo
- validar nombres y formato

Para el lienzo visual del reporte, usa el blueprint de `report/dashboard_spec.md` y termina el armado en Power BI Desktop.
