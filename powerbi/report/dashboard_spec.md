# Dashboard Spec

## Página 1: Resumen Ejecutivo

Objetivo: ver estado operativo del día en 15 segundos.

Visuales:

- Tarjeta: `Contenedores`
- Tarjeta: `Contenedores En Puerto`
- Tarjeta: `Contenedores En Patio`
- Tarjeta: `Contenedores Entregados`
- Tarjeta: `Alertas CAS`
- Tarjeta: `CAS Vencidos`
- Gauge o KPI: `Pct Cumplimiento Grupasa`
- Gauge o KPI: `Pct Cumplimiento Galagans`
- Columna apilada: contenedores por `status_actual`
- Barra horizontal: contenedores por `naviera`
- Tabla corta:
  - `contenedor_id`
  - `pedido`
  - `puerto`
  - `naviera`
  - `fecha_cas`
  - `status_actual`
  - `alerta_cas`
  - `cas_vencido`

Filtros:

- `fecha_snapshot`
- `puerto`
- `naviera`
- `status_actual`

## Página 2: Seguimiento Operativo

Objetivo: priorizar la operación del día.

Visuales:

- Matriz de detalle con:
  - `contenedor_id`
  - `pedido`
  - `parcial`
  - `puerto`
  - `naviera`
  - `deposito_vacio`
  - `plan_llegada_grupasa`
  - `plan_llegada_patio`
  - `plan_devolucion_vacio`
  - `status_actual`
  - `tipo_incidencia`
  - `comentario_status`
- Segmentador: `cumplimiento_grupasa`
- Segmentador: `cumplimiento_galagans`
- Segmentador: `tipo_incidencia`

Formato recomendado:

- resaltar `cas_vencido` en rojo
- resaltar `alerta_cas` en amarillo
- resaltar `ENTREGADO` en verde

## Página 3: Tiempos y Cumplimiento

Objetivo: medir desempeño logístico.

Visuales:

- Tarjeta: `Dias Prom Puerto a Patio`
- Tarjeta: `Dias Prom Patio a Bodega`
- Tarjeta: `Dias Prom Bodega a Deposito`
- Barra agrupada: promedio de días por `naviera`
- Barra agrupada: promedio de días por `puerto`
- Columna: conteo por `cumplimiento_grupasa`
- Columna: conteo por `cumplimiento_galagans`
- Scatter opcional:
  - eje X: `dias_puerto_a_patio`
  - eje Y: `dias_patio_a_bodega`
  - tamaño: contenedores
  - leyenda: `naviera`

## Página 4: Evolución Histórica

Objetivo: ver cómo cambia el status por día.

Visuales:

- Línea: conteo diario por `fecha_snapshot`
- Área apilada: contenedores por `status_actual` y `fecha_snapshot`
- Heatmap o matriz: `fecha_snapshot` vs `status_actual`
- Tabla detalle histórica:
  - `fecha_snapshot`
  - `contenedor_id`
  - `status_actual`
  - `tipo_incidencia`
  - `comentario_status`

Fuente principal:

- `FactStatusHistorico`

## Página 5: Calidad de Datos

Objetivo: monitorear salud del pipeline.

Visuales:

- Tarjeta: `Errores Validacion`
- Barra: errores por `severity`
- Barra: errores por `error_code`
- Tabla:
  - `fecha_snapshot`
  - `sheet_name`
  - `severity`
  - `error_code`
  - `contenedor_id`
  - `detail`

## Navegación

Orden sugerido:

1. Resumen Ejecutivo
2. Seguimiento Operativo
3. Tiempos y Cumplimiento
4. Evolución Histórica
5. Calidad de Datos
