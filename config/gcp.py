import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "insurance-data-platform-500423")
GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", "insurance-data-platform-reports")
BQ_DATASET = os.getenv("GCP_BQ_DATASET", "insurance_dw")

bq_client: bigquery.Client | None = None
gcs_client: storage.Client | None = None


def get_bq() -> bigquery.Client:
    global bq_client
    if bq_client is None:
        bq_client = bigquery.Client(project=PROJECT_ID)
    return bq_client


def get_gcs() -> storage.Client:
    global gcs_client
    if gcs_client is None:
        gcs_client = storage.Client(project=PROJECT_ID)
    return gcs_client


def get_bucket() -> storage.Bucket:
    return get_gcs().bucket(GCS_BUCKET)


def upload_to_gcs(local_path: str, blob_name: str) -> str:
    bucket = get_bucket()
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    return f"gs://{GCS_BUCKET}/{blob_name}"


def upload_bytes_to_gcs(data: bytes, blob_name: str, content_type: str = "application/octet-stream") -> str:
    bucket = get_bucket()
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type=content_type)
    return f"gs://{GCS_BUCKET}/{blob_name}"


def dump_df_to_gcs(df, prefix: str) -> str:
    import pandas as pd
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    blob_name = f"data-lake/{prefix}_{ts}.parquet"
    return upload_bytes_to_gcs(buf.getvalue(), blob_name, "application/parquet")
