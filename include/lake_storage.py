"""
lake_storage.py — object storage wrapper for the "lake" layer.

Two backends, selected by LAKE_BACKEND env var:

  - "minio" (default) — local MinIO (S3-compatible), zero external
    dependencies, safe for `make up` / first-time testing.
  - "gcs" — real Google Cloud Storage. Uses Application Default
    Credentials (works with either a bootcamp-issued user login via
    `gcloud auth application-default login`, or a service account key —
    google-auth handles both the same way once GOOGLE_APPLICATION_
    CREDENTIALS points at the right file).

Callers (the DAGs, the Spark job) never see this distinction — they
only call the four functions exported here, so nothing else in the
pipeline changes when you flip LAKE_BACKEND.

Bucket layout is identical either way:
    landing/articles/dt=YYYY-MM-DD/HHMMSS.parquet   <- raw, as scraped
    cleaned/articles/dt=YYYY-MM-DD/HHMMSS_*.parquet  <- after PySpark job
"""
import os

BACKEND = os.environ.get("LAKE_BACKEND", "minio").lower()


if BACKEND == "gcs":
    from google.api_core.exceptions import Forbidden, NotFound
    from google.cloud import storage as gcs_storage

    _client = None

    def _gcs_client():
        global _client
        if _client is None:
            _client = gcs_storage.Client(project=os.environ.get("GCP_PROJECT") or None)
        return _client

    def ensure_bucket(bucket: str) -> None:
        """
        Bootcamp-issued buckets are usually pre-provisioned — this just
        verifies access first, and only attempts to create the bucket
        if it's actually missing (and raises a clear error, not a raw
        stack trace, if the account can't create one either).
        """
        client = _gcs_client()
        try:
            client.get_bucket(bucket)
            return
        except NotFound:
            pass
        except Forbidden as e:
            raise RuntimeError(
                f"No permission to access GCS bucket '{bucket}'. Check GCP_PROJECT "
                f"and that the logged-in account has Storage access to it."
            ) from e

        try:
            client.create_bucket(bucket)
        except Exception as e:
            raise RuntimeError(
                f"GCS bucket '{bucket}' does not exist and could not be created "
                f"({e}). Create it manually in the GCP console, or fix LAKE_BUCKET."
            ) from e

    def upload_bytes(bucket: str, key: str, data: bytes) -> None:
        ensure_bucket(bucket)
        _gcs_client().bucket(bucket).blob(key).upload_from_string(data)

    def download_bytes(bucket: str, key: str) -> bytes:
        return _gcs_client().bucket(bucket).blob(key).download_as_bytes()

    def list_prefix(bucket: str, prefix: str) -> list[str]:
        return [b.name for b in _gcs_client().list_blobs(bucket, prefix=prefix)]

else:
    import io

    import boto3
    from botocore.client import Config

    def _client():
        return boto3.client(
            "s3",
            endpoint_url=os.environ["LAKE_ENDPOINT_URL"],
            aws_access_key_id=os.environ["LAKE_ACCESS_KEY"],
            aws_secret_access_key=os.environ["LAKE_SECRET_KEY"],
            config=Config(signature_version="s3v4"),
            region_name="asia-southeast2",
        )

    def ensure_bucket(bucket: str) -> None:
        s3 = _client()
        try:
            s3.create_bucket(Bucket=bucket)
        except s3.exceptions.BucketAlreadyOwnedByYou:
            pass
        except s3.exceptions.ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    def upload_bytes(bucket: str, key: str, data: bytes) -> None:
        ensure_bucket(bucket)
        _client().put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data))

    def download_bytes(bucket: str, key: str) -> bytes:
        obj = _client().get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def list_prefix(bucket: str, prefix: str) -> list[str]:
        s3 = _client()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
