-- ESP: Ejemplo BQML: predicción simple de demanda por sucursal/servicio (por día).
-- ENG: BQML example: simple daily demand forecasting per branch/service.

-- Vista de agregación diaria
CREATE OR REPLACE TABLE `${BQ_DATASET}.venta_diaria` AS
SELECT
  DATE(fecha_hora) AS fecha,
  sucursal_id,
  servicio_id,
  COUNTIF(estatus='completada') AS ventas_completadas,
  AVG(espera_min) AS espera_promedio
FROM `${BQ_DATASET}.fact_venta_cita_ext`
GROUP BY 1,2,3;

-- Modelo de regresión lineal (baseline)
CREATE OR REPLACE MODEL `${BQ_DATASET}.demanda_linear`
OPTIONS(model_type='linear_reg', input_label_cols=['ventas_completadas']) AS
SELECT
  ventas_completadas AS label,
  EXTRACT(DAYOFWEEK FROM fecha) AS dow,
  EXTRACT(WEEK FROM fecha) AS week,
  EXTRACT(MONTH FROM fecha) AS month,
  servicio_id,
  sucursal_id,
  espera_promedio
FROM `${BQ_DATASET}.venta_diaria`;

-- Predicción para próximos 14 días (naive calendar features)
CREATE OR REPLACE TABLE `${BQ_DATASET}.pred_demanda_14d` AS
WITH cal AS (
  SELECT DATE_ADD(MAX(fecha), INTERVAL off DAY) AS fecha
  FROM `${BQ_DATASET}.venta_diaria`, UNNEST(GENERATE_ARRAY(1, 14)) AS off
),
grid AS (
  SELECT c.fecha, s.sucursal_id, v.servicio_id
  FROM cal c
  CROSS JOIN (SELECT DISTINCT sucursal_id FROM `${BQ_DATASET}.venta_diaria`) s
  CROSS JOIN (SELECT DISTINCT servicio_id FROM `${BQ_DATASET}.venta_diaria`) v
)
SELECT
  g.*,
  pred.value AS pred_ventas
FROM ML.PREDICT(MODEL `${BQ_DATASET}.demanda_linear`, (
  SELECT
    EXTRACT(DAYOFWEEK FROM fecha) AS dow,
    EXTRACT(WEEK FROM fecha) AS week,
    EXTRACT(MONTH FROM fecha) AS month,
    servicio_id,
    sucursal_id,
    20.0 AS espera_promedio -- proxy
  FROM grid g
)) AS pred
JOIN grid g ON TRUE;
