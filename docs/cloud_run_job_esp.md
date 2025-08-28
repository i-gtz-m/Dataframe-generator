# ESP — Alternativa sin Airflow: Cloud Run Jobs + Scheduler

1) Crea una imagen con `tools/upload_folder_to_gcs.py` y un pequeño `ENTRYPOINT` que lea variables de entorno:
   - `LOCAL_DIR` (montado vía volumen si corres on-prem o Cloud Run con Cloud Storage FUSE si ya están en GCS).
   - Para acceso directo a OneDrive, considera compartir enlaces públicos y bajarlos primero con `curl`.
   (El acceso con Microsoft Graph requiere app/consentimiento y es más complejo; empieza con Airflow local y carpeta sincronizada).

2) Despliega como Job:
```bash
gcloud run jobs create sd-upload-job \
  --image=gcr.io/$PROJECT/sd-upload:latest \
  --set-env-vars=BUCKET=$GCS_BUCKET,LOCAL_DIR=/app/input \
  --region=us-central1
```

3) Programa con **Cloud Scheduler** (diario):
```bash
gcloud scheduler jobs create http sd-upload-daily \
  --schedule="0 2 * * *" \
  --uri="$(gcloud run jobs describe sd-upload-job --region=us-central1 --format='value(uri)')" \
  --http-method=POST
```
