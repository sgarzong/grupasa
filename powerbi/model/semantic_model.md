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

- slicers por naviera, puerto, deposito, pedido y parcial
- dimension principal para ambas tablas de hechos

### `DimFecha`

Grano: una fila por fecha.

Uso:

- filtros temporales
- tendencias
- relaciones activas e inactivas con la tabla de plan actual

### `DimStatus`

Grano: una fila por status.

Uso:

- slicers por status
- agrupacion por etapa derivada (`PUERTO`, `PATIO`, `BODEGA`, `DEPOSITO`)

### `DimBodega`

Grano: una fila por bodega.

Uso:

- slicers y segmentacion operativa

### `FactStatusDiario`

Grano: una fila por contenedor por `fecha_snapshot`.

Uso:

- tendencia diaria por status
- evolucion del contenedor
- alertas CAS historicas
- incidencias por dia

### `FactPlanActual`

Grano: una fila por contenedor en la foto mas reciente.

Uso:

- KPIs ejecutivos
- cumplimiento Grupasa / Galagans
- tiempos entre etapas
- detalle operativo actual

### `FactErroresValidacion`

Grano: una fila por error de validacion.

Uso:

- pagina de calidad de datos
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

- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_arribo_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[fecha_cas_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_llegada_grupasa_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_llegada_patio_key]`
- `DimFecha[fecha_key]` 1:* `FactPlanActual[plan_devolucion_vacio_key]`

Recomendacion:

- relaciones de filtro simple desde dimensiones a hechos
- no crear relaciones directas entre hechos
- usar `USERELATIONSHIP` cuando necesites activar una fecha alternativa en medidas
