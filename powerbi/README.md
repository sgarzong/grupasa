# Power BI Dashboard Pack

Este paquete deja listo el diseño funcional del dashboard para los CSV generados por el pipeline:

- `data/curated/contenedores_actual.csv`
- `data/history/status_historico.csv`
- `data/history/registro_congelado.csv`
- `data/history/plan_galagans_congelado.csv`
- `data/quality/errores_validacion.csv`

## Qué incluye

- `theme/grupasa_theme.json`: tema visual para el reporte.
- `model/semantic_model.md`: modelo recomendado y relaciones.
- `model/measures.dax`: medidas DAX base para KPIs, cumplimiento y calidad.
- `report/dashboard_spec.md`: blueprint de páginas y visuales.
- `mcp/prompts.md`: prompts listos para usar con Power BI Modeling MCP.

## Tablas recomendadas en Power BI

- `FactContenedoresActual`
- `FactStatusHistorico`
- `FactRegistroCongelado`
- `FactPlanGalagansCongelado`
- `FactErroresValidacion`
- `DimFecha`

## Flujo recomendado en Power BI Desktop

1. Importar los cinco CSV desde la carpeta `data`.
2. Renombrar tablas según la convención anterior.
3. Crear `DimFecha` con DAX.
4. Relacionar:
   - `FactContenedoresActual[fecha_snapshot]` -> `DimFecha[Fecha]`
   - `FactStatusHistorico[fecha_snapshot]` -> `DimFecha[Fecha]`
   - `FactRegistroCongelado[fecha_snapshot]` -> `DimFecha[Fecha]`
   - `FactPlanGalagansCongelado[fecha_snapshot]` -> `DimFecha[Fecha]`
5. Cargar las medidas de `model/measures.dax`.
6. Aplicar `theme/grupasa_theme.json`.
7. Construir las páginas según `report/dashboard_spec.md`.

## Sobre MCP

El Modeling MCP te sirve muy bien para:

- crear medidas
- ordenar campos
- definir tablas de fechas
- revisar nombres y formato
- documentar modelo

Para el lienzo visual del reporte, usa el blueprint de `report/dashboard_spec.md` y termina el armado en Power BI Desktop.
