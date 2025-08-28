# ENG — Quickstart

1) **Generate the dataset (≈1M total rows)** — same command as in ESP README but adjust path separators for Windows if needed.
2) **Upload to GCS** — Local Airflow DAG uploads all files in a local directory (synced from OneDrive) to a GCS bucket.
3) **BigQuery** — Use `sql/` scripts to create datasets and external tables (or load as native tables).
4) **ML** — Basic **BigQuery ML** examples in `ml/bqml_demo.sql` for branch/service demand forecasting.
5) **Costs** — GCS storage in cents/month for a few GB; BigQuery in cents per hundreds of queries if you prune by date; Airflow local is $0 in cloud; Cloud Run Jobs + Scheduler usually sits in cents/month for a daily run.
