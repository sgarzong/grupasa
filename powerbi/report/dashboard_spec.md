# Dashboard Spec

## Pagina 1: Resumen Ejecutivo

Objetivo: ver estado operativo del dia en 15 segundos.

Visuales:

- Tarjeta: `Contenedores`
- Tarjeta: `Contenedores En Puerto`
- Tarjeta: `Contenedores En Patio`
- Tarjeta: `Contenedores En Bodega`
- Tarjeta: `Alertas CAS`
- Tarjeta: `CAS Vencidos`
- KPI: `Pct Cumplimiento Grupasa`
- KPI: `Pct Cumplimiento Galagans`
- Columna apilada: contenedores por `DimStatus[status_actual]`
- Barra horizontal: contenedores por `DimContenedor[naviera]`
- Tabla corta:
  - `FactPlanActual[contenedor_id]`
  - `DimContenedor[pedido]`
  - `DimContenedor[puerto]`
  - `DimContenedor[naviera]`
  - `FactPlanActual[fecha_cas]`
  - `DimStatus[status_actual]`
  - `FactPlanActual[alerta_cas]`
  - `FactPlanActual[cas_vencido]`

Filtros:

- `FactPlanActual[fecha_snapshot]`
- `DimContenedor[puerto]`
- `DimContenedor[naviera]`
- `DimStatus[status_actual]`

## Pagina 2: Seguimiento Operativo

Objetivo: priorizar la operacion del dia.

Visuales:

- Matriz de detalle con:
  - `FactPlanActual[contenedor_id]`
  - `DimContenedor[pedido]`
  - `DimContenedor[parcial]`
  - `DimContenedor[puerto]`
  - `DimContenedor[naviera]`
  - `DimContenedor[deposito_vacio]`
  - `FactPlanActual[plan_llegada_grupasa]`
  - `FactPlanActual[plan_llegada_patio]`
  - `FactPlanActual[plan_devolucion_vacio]`
  - `DimStatus[status_actual]`
  - `FactStatusDiario[tipo_incidencia]`
  - `FactStatusDiario[comentario_status]`
- Segmentador: `FactPlanActual[cumplimiento_grupasa]`
- Segmentador: `FactPlanActual[cumplimiento_galagans]`
- Segmentador: `FactStatusDiario[tipo_incidencia]`

Formato recomendado:

- resaltar `cas_vencido` en rojo
- resaltar `alerta_cas` en amarillo
- resaltar etapa `BODEGA` o `ENTREGADO` en verde

## Pagina 3: Tiempos y Cumplimiento

Objetivo: medir desempeno logistico.

Visuales:

- Tarjeta: `Dias Prom Puerto a Patio`
- Tarjeta: `Dias Prom Patio a Bodega`
- Tarjeta: `Dias Prom Bodega a Deposito`
- Barra agrupada: promedio de dias por `DimContenedor[naviera]`
- Barra agrupada: promedio de dias por `DimContenedor[puerto]`
- Columna: conteo por `FactPlanActual[cumplimiento_grupasa]`
- Columna: conteo por `FactPlanActual[cumplimiento_galagans]`

## Pagina 4: Evolucion Historica

Objetivo: ver como cambia el status por dia.

Visuales:

- Linea: conteo diario por `DimFecha[fecha]`
- Area apilada: contenedores por `DimStatus[status_actual]` y `DimFecha[fecha]`
- Matriz: `DimFecha[fecha]` vs `DimStatus[status_actual]`
- Tabla detalle historica:
  - `FactStatusDiario[fecha_snapshot]`
  - `FactStatusDiario[contenedor_id]`
  - `DimStatus[status_actual]`
  - `FactStatusDiario[tipo_incidencia]`
  - `FactStatusDiario[comentario_status]`

Fuente principal:

- `FactStatusDiario`

## Pagina 5: Calidad de Datos

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

## Navegacion

Orden sugerido:

1. Resumen Ejecutivo
2. Seguimiento Operativo
3. Tiempos y Cumplimiento
4. Evolucion Historica
5. Calidad de Datos
