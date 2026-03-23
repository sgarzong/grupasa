# Semantic Model

## Objetivo

Construir un dashboard operativo para monitorear el flujo:

- Puerto -> Patio Galagans
- Patio Galagans -> Bodega Grupasa
- Bodega Grupasa -> Depósito vacío

## Tablas

### `FactContenedoresActual`

Grano: una fila por contenedor en el snapshot más reciente.

Uso:

- KPIs ejecutivos
- semáforos CAS
- cumplimiento Grupasa / Galagans
- detalle operativo actual

### `FactStatusHistorico`

Grano: una fila por contenedor por fecha_snapshot.

Uso:

- tendencia diaria por status
- reconstrucción de evolución del contenedor
- análisis temporal

### `FactRegistroCongelado`

Grano: una fila por contenedor por fecha_snapshot.

Uso:

- auditoría de cambios en datos maestros
- evolución de CAS, depósito, puerto, producto

### `FactPlanGalagansCongelado`

Grano: una fila por contenedor por fecha_snapshot.

Uso:

- auditoría de cambios de planificación Galagans
- seguimiento de plan de llegada patio y devolución vacío

### `FactErroresValidacion`

Grano: una fila por error de validación.

Uso:

- página de calidad de datos
- monitoreo del pipeline

### `DimFecha`

Grano: una fila por fecha.

Uso:

- segmentación temporal
- filtros y tendencias

## Relaciones

- `DimFecha[Fecha]` 1:* `FactContenedoresActual[fecha_snapshot]`
- `DimFecha[Fecha]` 1:* `FactStatusHistorico[fecha_snapshot]`
- `DimFecha[Fecha]` 1:* `FactRegistroCongelado[fecha_snapshot]`
- `DimFecha[Fecha]` 1:* `FactPlanGalagansCongelado[fecha_snapshot]`
- `DimFecha[Fecha]` 1:* `FactErroresValidacion[fecha_snapshot]`

Recomendación:

- relaciones de fecha en dirección simple desde `DimFecha`
- no crear relaciones físicas por `contenedor_id` entre hechos
- usar medidas o drillthrough para cruces operativos

## Columnas clave para slicing

- `puerto`
- `naviera`
- `deposito_vacio`
- `bodega`
- `status_actual`
- `tipo_incidencia`
- `cumplimiento_grupasa`
- `cumplimiento_galagans`

## Tabla de fecha sugerida

```DAX
DimFecha =
ADDCOLUMNS(
    CALENDAR(DATE(2026, 1, 1), DATE(2028, 12, 31)),
    "Anio", YEAR([Date]),
    "MesNumero", MONTH([Date]),
    "Mes", FORMAT([Date], "MMM"),
    "AnioMes", FORMAT([Date], "YYYY-MM"),
    "Trimestre", "T" & FORMAT([Date], "Q"),
    "Dia", DAY([Date]),
    "Semana", WEEKNUM([Date], 2)
)
```

Luego renombrar `Date` a `Fecha`.
