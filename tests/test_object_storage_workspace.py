from pathlib import Path
from typing import Set

from openeogeotrellis.workspace import ObjectStorageWorkspace


def test_import_file(mock_s3_client, mock_s3_bucket):
    some_file = Path(__file__)
    merge = "some/target"

    workspace = ObjectStorageWorkspace(bucket="openeo-fake-bucketname")
    workspace.import_file(some_file, merge=merge)

    assert _workspace_keys(mock_s3_client, workspace.bucket, merge) == {f"{merge}/{some_file.name}"}


def test_import_object(mock_s3_client, mock_s3_bucket):
    some_file = Path(__file__)
    source_bucket = target_bucket = "openeo-fake-bucketname"
    source_key = "some/source/object"
    merge = "some/target"
    assert source_key != merge

    with open(some_file, "rb") as f:
        mock_s3_client.put_object(Bucket=source_bucket, Key=source_key, Body=f.read())

    assert _workspace_keys(mock_s3_client, source_bucket, prefix=source_key) == {source_key}

    workspace = ObjectStorageWorkspace(bucket=target_bucket)
    workspace.import_object(f"s3://{source_bucket}/{source_key}", merge=merge)

    assert _workspace_keys(mock_s3_client, workspace.bucket, merge) == {"some/target/object"}


def _workspace_keys(s3_client, bucket, prefix) -> Set[str]:
    return {obj["Key"] for obj in s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])}
