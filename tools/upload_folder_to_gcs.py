#!/usr/bin/env python
import os
import argparse
from google.cloud import storage

def upload_folder(local_dir, bucket_name, prefix="raw/"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for root, _, files in os.walk(local_dir):
        for fname in files:
            if fname.endswith((".parquet", ".csv")):
                local_path = os.path.join(root, fname)
                rel = os.path.relpath(local_path, local_dir).replace("\\", "/")
                blob = bucket.blob(f"{prefix}{rel}")
                blob.upload_from_filename(local_path)
                print(f"Uploaded {local_path} -> gs://{bucket_name}/{prefix}{rel}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-dir", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="raw/")
    args = parser.parse_args()
    upload_folder(args.local_dir, args.bucket, args.prefix)
