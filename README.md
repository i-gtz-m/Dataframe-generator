# Cadena comercial de servicios de salud, Synthetic Analytics Stack (ESP/ENG)

**ESP.** Proyecto de referencia para generar datos sintéticos realistas de una red de 100 sucursales (clientes, empleados, servicios, inventarios y ventas/citas), cargarlos a OneDrive, y orquestar su envío barato a Google Cloud (GCS + BigQuery). Incluye un DAG de Airflow local para subir a GCS y scripts para crear tablas/vistas. Opcional: usar Cloud Run Jobs en vez de Airflow.

**ENG.** Reference project to generate realistic synthetic data for a 100-branch network (customers, employees, services, inventory and sales/appointments), save to OneDrive, and orchestrate a low-cost path to Google Cloud (GCS + BigQuery). Includes a local Airflow DAG to upload to GCS and scripts to create tables/views. Optional: use Cloud Run Jobs instead of Airflow.

---

## ESP — Guía rápida

### 1) Generar el dataset (≈1M filas totales)
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python data_generator/generate_dataset.py \
  --output-dir data_generator/output \
  --seed 42 \
  --n-sucursales 100 \
  --n-clientes 150000 \
  --n-empleados 8000 \
  --n-servicios 40 \\
  --n-insumos 200 \\
  --n-ventas 650000 \\
  --n-inv-mov 150000 \\
  --dias-snapshot 6
```

Esto producirá archivos **Parquet** (y muestras CSV) en `data_generator/output/`. Coloca esa carpeta dentro de tu carpeta sincronizada de **OneDrive** (o copia los archivos).

### 2) Subir a Google Cloud
- **Opción A (recomendada): Airflow local** que lee una carpeta local (sincronizada por OneDrive) y sube a **GCS**.
  1. Autentícate con GCP:
     ```bash
     gcloud auth application-default login
     gcloud config set project <TU_PROYECTO>
     ```
  2. Define variables de entorno para el DAG:
     ```bash
     export GCP_PROJECT=<TU_PROYECTO>
     export GCS_BUCKET=<tu-bucket>
     export LOCAL_INPUT_DIR=<ruta-a-tu-onedrive>/SD_project/data_generator/output
     ```
  3. Inicia Airflow local apuntando `AIRFLOW_HOME=${PWD}/airflow_home` y coloca el DAG en `dags/`:
     ```bash
     export AIRFLOW_HOME=${PWD}/airflow_home
     mkdir -p "$AIRFLOW_HOME/dags"
     cp -r dags/local/* "$AIRFLOW_HOME/dags/"
     airflow db init
     airflow users create --username admin --password admin --firstname a --lastname b --role Admin --email a@b.c
     airflow webserver -p 8080 &
     airflow scheduler &
     ```
  4. En la UI, habilita el DAG **local_to_gcs_dataset**.

- **Opción B (alternativa sin Airflow):** Empaqueta `tools/upload_folder_to_gcs.py` en un contenedor y ejecútalo como **Cloud Run Job** con **Cloud Scheduler**. (Más detalles en `docs/cloud_run_job_esp.md`).

### 3) BigQuery
Ejecuta los scripts de `sql/` para crear datasets y tablas externas (o cargar a tablas nativas).

### 4) ML (demo)
En `ml/bqml_demo.sql` hay ejemplos de **BigQuery ML** para una predicción básica de demanda por sucursal/servicio.

### 5) Costos (estimación)
- GCS (unos GB): centavos/mes.
- BigQuery: con tablas externas y consultas filtradas por fecha, centavos por cientos de consultas.
- Orquestación: Airflow local = $0 en cloud; Cloud Run Jobs + Scheduler: centavos/mes con una ejecución diaria.

---

## ENG — Quickstart

See `docs/README_eng.md` for the English guide. Steps mirror the Spanish section.
