import boto3
from botocore.exceptions import ClientError

from app.config import config

_s3_client: boto3.client | None = None


def get_s3_client() -> boto3.client:
    global _s3_client
    if _s3_client is None:
        kwargs: dict = {"region_name": config.aws_region}
        if config.aws_endpoint_url:
            kwargs["endpoint_url"] = config.aws_endpoint_url
        _s3_client = boto3.client("s3", **kwargs)
    return _s3_client


def fetch_jsonl_from_s3(bucket: str, key: str) -> bytes:
    """Fetch object from S3. Key can be a prefix (ends with /) or exact key."""
    client = get_s3_client()
    if key.endswith("/"):
        # List objects under prefix, fetch first .jsonl found
        resp = client.list_objects_v2(Bucket=bucket, Prefix=key)
        contents = resp.get("Contents") or []
        jsonl_keys = [c["Key"] for c in contents if c["Key"].endswith(".jsonl")]
        if not jsonl_keys:
            msg = f"No .jsonl files under s3://{bucket}/{key}"
            raise FileNotFoundError(msg)
        key = jsonl_keys[0]
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            msg = f"No object at s3://{bucket}/{key}"
            raise FileNotFoundError(msg) from e
        raise
    return resp["Body"].read()


def list_jsonl_keys(bucket: str, prefix: str) -> list[str]:
    """List all .jsonl object keys under prefix."""
    client = get_s3_client()
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = resp.get("Contents") or []
    return [c["Key"] for c in contents if c["Key"].endswith(".jsonl")]
