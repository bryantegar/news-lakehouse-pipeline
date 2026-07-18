"""
lake_storage.py — object storage wrapper for the "lake" layer.

Locally this talks to MinIO (S3-compatible), which is the free,
laptop-friendly stand-in for Google Cloud Storage used in the GCS
bootcamp assignment. Bucket layout mirrors what you'd actually use
on GCS in the `asia-southeast2` region:

    landing/articles/dt=YYYY-MM-DD/HH/*.parquet   <- raw, as scraped
    cleaned/articles/dt=YYYY-MM-DD/HH/*.parquet    <- after PySpark job

To swap to real GCS: replace the boto3 client below with
`google.cloud.storage.Client()` and change `upload_parquet` /
`download_prefix` to use `blob.upload_from_filename` /
`bucket.list_blobs`. Callers (the DAGs, the Spark job) do not change —
they only call the four functions exported here.
"""
import os
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
