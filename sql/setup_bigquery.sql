-- ESP/ENG: Create dataset and external tables on Parquet in GCS.
-- Set variables before running in your shell:
--   export BQ_DATASET=analytics_iot   (or another name)
--   export GCS_BUCKET=<your-bucket>
-- Then:
--   bq --location=US mk -d $BQ_DATASET

-- External tables
-- Example: dim_sucursal
CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.dim_sucursal_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${GCS_BUCKET}/raw/dim_sucursal.parquet']
);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.dim_servicio_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/dim_servicio.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.dim_insumo_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/dim_insumo.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.dim_cliente_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/dim_cliente.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.dim_empleado_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/dim_empleado.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.fact_venta_cita_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/fact_venta_cita.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.fact_inventario_mov_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/fact_inventario_mov.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `${BQ_DATASET}.snap_inventario_diario_ext`
OPTIONS (format='PARQUET', uris=['gs://${GCS_BUCKET}/raw/snap_inventario_diario.parquet']);
