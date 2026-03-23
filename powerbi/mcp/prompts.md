# MCP Prompts

Usa estos prompts dentro de tu cliente conectado a Power BI Modeling MCP.

## 1. Crear modelo base

```text
En este proyecto de Power BI crea o ajusta las tablas importadas desde CSV con estos nombres:
FactContenedoresActual, FactStatusHistorico, FactRegistroCongelado, FactPlanGalagansCongelado y FactErroresValidacion.
Crea además una DimFecha con columna Fecha y marca la tabla como date table.
Relaciona DimFecha[Fecha] con la columna fecha_snapshot de cada tabla de hechos con filtro simple.
No crees relaciones físicas entre hechos por contenedor_id.
```

## 2. Crear medidas

```text
Crea las medidas DAX definidas en el archivo powerbi/model/measures.dax.
Agrúpalas en una tabla de medidas llamada _Measures.
Aplica formato de porcentaje a Pct Cumplimiento Grupasa y Pct Cumplimiento Galagans.
Aplica formato decimal con 1 decimal a los promedios de días.
```

## 3. Ordenar campos

```text
En FactContenedoresActual organiza los campos para que primero aparezcan:
contenedor_id, pedido, parcial, naviera, puerto, deposito_vacio, fecha_arribo, fecha_cas,
plan_llegada_grupasa, bodega, hora_descarga, plan_llegada_patio, plan_devolucion_vacio,
status_actual, tipo_incidencia, comentario_status, alerta_cas, cas_vencido,
dias_puerto_a_patio, dias_patio_a_bodega, dias_bodega_a_deposito,
cumplimiento_grupasa, cumplimiento_galagans.
Oculta columnas técnicas que no aporten al usuario final.
```

## 4. Crear página ejecutiva

```text
Genera una propuesta de página 'Resumen Ejecutivo' para Power BI con KPIs de contenedores, alertas CAS,
cumplimiento Grupasa y Galagans, distribución por status y naviera, usando FactContenedoresActual.
Sugiere visuales, campos y disposición en cuadrícula de 16:9.
```

## 5. Validar el modelo

```text
Revisa el modelo semántico y detecta problemas de tipos de datos, columnas mal tipadas como texto,
o medidas que deberían usar DIVIDE. Prioriza fecha_snapshot, fecha_cas, fecha_arribo,
plan_llegada_grupasa, plan_llegada_patio y plan_devolucion_vacio.
```
