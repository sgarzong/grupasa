# Semantic Model

## Objetivo

Construir un dashboard operativo para monitorear el flujo:

- Puerto -> Patio Galagans
- Patio Galagans -> Bodega Grupasa
- Bodega Grupasa -> Deposito vacio

## Tablas

### `DimContenedor`

Grano: una fila por contenedor.

Uso:

- slicers por pedido, parcial, naviera, puerto y deposito
- dimension principal para ambos hechos

### `DimFecha`

Grano: una fila por fecha.

Uso:

- filtros temporales
- tendencias
- relaciones activas e inactivas con `FactPlanActual`

### `DimStatus`

Grano: una fila por status.

Uso:

- slicers por status
- agrupacion por etapa derivada

### `DimBodega`

Grano: una fila por bodega.

Uso:

- slicers y segmentacion operativa

### `FactStatusDiario`

Grano: una fila por contenedor por `fecha_snapshot`.

Uso:

- evolucion diaria de status
- incidencias por dia
- alertas CAS historicas

### `FactPlanActual`

Grano: una fila por contenedor en la foto vigente.

Uso:

- KPIs ejecutivos
- cumplimiento
- tiempos entre etapas
- detalle actual
- analisis de reasignacion de slots Grupasa

Campos clave de esta version:

- `fecha_arribo_gye`
- `fecha_salida_autorizada`
- `plan_slot_grupasa`
- `tipo_asignacion_grupasa`

### `FactErroresValidacion`

Grano: una fila por error de validacion.

Uso:

- pagina de calidad
- monitoreo del pipeline

## Relaciones

- `DimContenedor[contenedor_key]` 1:* `FactStatusDiario[contenedor_key]`
- `DimContenedor[contenedor_key]` 1:* `FactPlanActual[contenedor_key]`
- `DimStatus[status_key]` 1:* `FactStatusDiario[status_key]`
- `DimStatus[status_key]` 1:* `FactPlanActual[status_key]`
- `DimBodega[bodega_key]` 1:* `FactStatusDiario[bodega_key]`
- `DimBodega[bodega_key]` 1:* `FactPlanActual[bodega_key]`
- `DimFecha[fecha_key]` 1:* `FactStatusDiario[fecha_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[snapshot_fecha_key]`

Relaciones recomendadas como inactivas:

- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_arribo_gye_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_salida_autorizada_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_arribo_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_cas_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_llegada_grupasa_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_llegada_patio_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_devolucion_vacio_key]`

## Recomendacion

- relaciones de filtro simple desde dimensiones a hechos
- no crear relaciones directas entre hechos
- usar `USERELATIONSHIP` para activar fechas alternativas en medidas
- usar `tipo_asignacion_grupasa` para separar contenedores reasignados vs plan directo
