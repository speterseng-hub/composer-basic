-- sql/metricas_diarias.sql
-- ---------------------------------------------------------------------------
-- Calcula las métricas diarias de órdenes para la fecha @fecha y las inserta
-- en la tabla daily_metrics. Se ejecuta desde dag_4_metricas vía
-- BigQueryInsertJobOperator, con @fecha = {{ ds }}.
-- ---------------------------------------------------------------------------

SELECT
  @fecha AS fecha,
  COUNT(*) AS total_orders,
  SUM(total_amount) AS total_revenue,
  AVG(total_amount) AS avg_order_value,
  COUNTIF(estado = 'pending') AS orders_pending,
  COUNTIF(estado = 'shipped') AS orders_shipped,
  COUNTIF(estado = 'delivered') AS orders_delivered,
  COUNTIF(estado = 'cancelled') AS orders_cancelled
FROM
  `{{ params.orders_table }}`
WHERE
  DATE(fecha) = @fecha