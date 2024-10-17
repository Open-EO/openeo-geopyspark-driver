from typing import Set

import pytest


from openeogeotrellis.workspace import ObjectStorageWorkspace


@pytest.mark.parametrize("remove_original", [False, True])
def test_import_file(tmp_path, mock_s3_client, mock_s3_bucket, remove_original):
    source_directory = tmp_path / "src"
    source_directory.mkdir()
    source_file = source_directory / "file"
    source_file.touch()

    merge = "some/target"

    workspace = ObjectStorageWorkspace(bucket="openeo-fake-bucketname")
    workspace.import_file(source_file, merge=merge, remove_original=remove_original)

    assert _workspace_keys(mock_s3_client, workspace.bucket, prefix=merge) == {f"{merge}/{source_file.name}"}
    assert source_file.exists() != remove_original


@pytest.mark.parametrize("remove_original", [False, True])
def test_import_object(tmp_path, mock_s3_client, mock_s3_bucket, remove_original):
    source_bucket = target_bucket = "openeo-fake-bucketname"
    source_key = "some/source/object"
    merge = "some/target"
    assert source_key != merge

    source_directory = tmp_path / "src"
    source_directory.mkdir()
    source_file = source_directory / "file"
    source_file.touch()

    with open(source_file, "rb") as f:
        mock_s3_client.put_object(Bucket=source_bucket, Key=source_key, Body=f.read())

    assert _workspace_keys(mock_s3_client, source_bucket, prefix=source_key) == {source_key}

    workspace = ObjectStorageWorkspace(bucket=target_bucket)
    workspace.import_object(f"s3://{source_bucket}/{source_key}", merge=merge, remove_original=remove_original)

    assert _workspace_keys(mock_s3_client, workspace.bucket, prefix=merge) == {"some/target/object"}

    if remove_original:
        assert _workspace_keys(mock_s3_client, source_bucket) == {"some/target/object"}
    else:
        assert _workspace_keys(mock_s3_client, source_bucket) == {"some/source/object", "some/target/object"}


def _workspace_keys(s3_client, bucket, prefix=None) -> Set[str]:
    kwargs = dict(Bucket=bucket)
    if prefix:
        kwargs["Prefix"] = prefix

    return {obj["Key"] for obj in s3_client.list_objects_v2(**kwargs).get("Contents", [])}
