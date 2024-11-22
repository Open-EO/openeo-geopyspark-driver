import datetime as dt
from pathlib import PurePath
from typing import Set

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item
import pytest

from openeogeotrellis.workspace import ObjectStorageWorkspace, CustomStacIO


@pytest.mark.parametrize("remove_original", [False, True])
def test_import_file(tmp_path, mock_s3_client, mock_s3_bucket, remove_original):
    source_directory = tmp_path / "src"
    source_directory.mkdir()
    source_file = source_directory / "file"
    source_file.touch()

    merge = "some/target"

    workspace = ObjectStorageWorkspace(bucket="openeo-fake-bucketname")
    workspace_uri = workspace.import_file(source_file, merge=merge, remove_original=remove_original)

    assert workspace_uri == f"s3://{workspace.bucket}/{merge}/{source_file.name}"
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
    workspace_uri = workspace.import_object(
        f"s3://{source_bucket}/{source_key}", merge=merge, remove_original=remove_original
    )

    assert workspace_uri == f"s3://{target_bucket}/some/target/object"
    assert _workspace_keys(mock_s3_client, workspace.bucket, prefix=merge) == {"some/target/object"}

    if remove_original:
        assert _workspace_keys(mock_s3_client, source_bucket) == {"some/target/object"}
    else:
        assert _workspace_keys(mock_s3_client, source_bucket) == {source_key, "some/target/object"}


def test_merge_new(mock_s3_client, mock_s3_bucket):
    target_bucket = "openeo-fake-bucketname"
    merge = PurePath("some/target/collection")

    workspace = ObjectStorageWorkspace(bucket=target_bucket)
    workspace.merge(_collection("collection", asset_filename="asset.tif"), merge)

    assert _workspace_keys(mock_s3_client, target_bucket) == {
        "some/target/collection",
        "some/target/collection/asset.tif/asset.tif.json",
    }

    custom_stac_io = CustomStacIO()

    collection_doc = custom_stac_io.read_text(f"s3://{target_bucket}/{merge}")
    print(collection_doc)

    item_doc = custom_stac_io.read_text(f"s3://{target_bucket}/{merge}/asset.tif/asset.tif.json")
    print(item_doc)

    exported_collection = Collection.from_file(f"s3://{target_bucket}/{merge}", stac_io=custom_stac_io)
    assert exported_collection.id == "collection"
    assert exported_collection.validate_all() == 1

    item = [item for item in exported_collection.get_items()][0]
    assert item.id == "asset.tif"
    assert item.get_self_href() == f"s3://{target_bucket}/{merge}/{item.id}/{item.id}.json"

    # TODO: add and check asset


def _collection(collection_id: str, asset_filename: str) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(SpatialExtent([[-180, -90, 180, 90]]), TemporalExtent([[None, None]])),
    )

    item = Item(id=asset_filename, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={})
    collection.add_item(item)

    return collection


def _workspace_keys(s3_client, bucket, prefix=None) -> Set[str]:
    kwargs = dict(Bucket=bucket)
    if prefix:
        kwargs["Prefix"] = prefix

    return {obj["Key"] for obj in s3_client.list_objects_v2(**kwargs).get("Contents", [])}
