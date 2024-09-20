from pathlib import Path

from openeo_driver.workspace import ObjectStorageWorkspace


def test_object_storage_workspace(mock_s3_client, mock_s3_bucket):
    some_file = Path(__file__)
    merge = "some/prefix"

    workspace = ObjectStorageWorkspace(bucket="openeo-fake-bucketname")
    workspace.import_file(some_file, merge=merge)

    workspace_keys = {
        obj["Key"] for obj in mock_s3_client.list_objects_v2(Bucket=workspace.bucket, Prefix=merge).get("Contents", [])
    }

    assert workspace_keys == {f"{merge}/{some_file.name}"}
