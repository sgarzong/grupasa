# Dashboard Spec

## Pagina 1: Resumen Ejecutivo

Objetivo: ver estado operativo del dia en segundos.

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
- Donut: contenedores por `FactPlanActual[tipo_asignacion_grupasa]`

## Pagina 2: Seguimiento Operativo

Objetivo: priorizar la operacion del dia.

Visuales:

- Matriz con:
  - `FactPlanActual[contenedor_id]`
  - `DimContenedor[pedido]`
  - `DimContenedor[parcial]`
  - `DimContenedor[puerto]`
  - `DimContenedor[naviera]`
  - `FactPlanActual[fecha_arribo_gye]`
  - `FactPlanActual[fecha_salida_autorizada]`
  - `FactPlanActual[fecha_arribo]`
  - `FactPlanActual[fecha_cas]`
  - `FactPlanActual[plan_llegada_grupasa]`
  - `FactPlanActual[plan_slot_grupasa]`
  - `FactPlanActual[tipo_asignacion_grupasa]`
  - `DimStatus[status_actual]`
  - `FactStatusDiario[tipo_incidencia]`
  - `FactStatusDiario[comentario_status]`

Filtros:

- `DimContenedor[puerto]`
- `DimContenedor[naviera]`
- `FactPlanActual[tipo_asignacion_grupasa]`
- `FactPlanActual[cumplimiento_grupasa]`
- `FactPlanActual[cumplimiento_galagans]`

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

## Pagina 4: Asignacion Grupasa

Objetivo: analizar la reasignacion de slots planificados por pedido.

Visuales:

- Tarjeta: `Contenedores Reasignados Grupasa`
- Tarjeta: `Contenedores Plan Directo Grupasa`
- Tabla:
  - `DimContenedor[pedido]`
  - `FactPlanActual[contenedor_id]`
  - `FactPlanActual[plan_slot_grupasa]`
  - `FactPlanActual[tipo_asignacion_grupasa]`
  - `FactPlanActual[plan_llegada_grupasa]`
  - `DimBodega[bodega]`
- Barra: conteo por `FactPlanActual[tipo_asignacion_grupasa]`

## Pagina 5: Evolucion Historica

Objetivo: ver como cambia el status por dia.

Visuales:

- Linea: conteo diario por `DimFecha[fecha]`
- Area apilada: contenedores por `DimStatus[status_actual]` y `DimFecha[fecha]`
- Matriz: `DimFecha[fecha]` vs `DimStatus[status_actual]`
- Tabla historica:
  - `FactStatusDiario[fecha_snapshot]`
  - `FactStatusDiario[contenedor_id]`
  - `DimStatus[status_actual]`
  - `FactStatusDiario[tipo_incidencia]`
  - `FactStatusDiario[comentario_status]`

## Pagina 6: Calidad de Datos

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
