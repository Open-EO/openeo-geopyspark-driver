import datetime as dt
from pathlib import PurePath, Path
from typing import Set, List
from urllib.parse import urlparse

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item, CatalogType, Asset
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


@pytest.mark.parametrize("remove_original", [False, True])
def test_merge_new(mock_s3_client, mock_s3_bucket, tmp_path, remove_original: bool):
    merge = PurePath("some/target/collection")

    source_directory = tmp_path / "src"
    source_directory.mkdir()
    disk_asset_path = source_directory / "disk_asset.tif"

    with open(disk_asset_path, "w") as f:
        f.write("disk_asset.tif\n")

    source_bucket = target_bucket = "openeo-fake-bucketname"
    source_key = "src/object_asset.tif"

    mock_s3_client.put_object(Bucket=source_bucket, Key=source_key, Body="object_asset.tif\n")

    new_collection = _collection(
        root_path=source_directory / "collection",
        collection_id="collection",
        asset_hrefs=[str(disk_asset_path), f"s3://{source_bucket}/src/object_asset.tif"],
        s3_client=mock_s3_client,
    )

    original_asset_path = [
        Path(asset.get_absolute_href()) for item in new_collection.get_items() for asset in item.get_assets().values()
    ][0]

    workspace = ObjectStorageWorkspace(bucket=target_bucket)
    exported_collection = workspace.merge(new_collection, merge, remove_original)

    assert isinstance(exported_collection, Collection)

    asset_workspace_uris = {
        asset_key: asset.extra_fields["alternate"]["s3"]
        for item in exported_collection.get_items()
        for asset_key, asset in item.get_assets().items()
    }

    assert asset_workspace_uris == {
        "disk_asset.tif": f"s3://{target_bucket}/{merge}/disk_asset.tif/disk_asset.tif",
        "object_asset.tif": f"s3://{target_bucket}/{merge}/object_asset.tif/object_asset.tif",
    }

    assert _workspace_keys(mock_s3_client, target_bucket, prefix="some/target/collection") == {
        "some/target/collection",
        "some/target/collection/disk_asset.tif/disk_asset.tif.json",
        "some/target/collection/disk_asset.tif/disk_asset.tif",
        "some/target/collection/object_asset.tif/object_asset.tif.json",
        "some/target/collection/object_asset.tif/object_asset.tif",
    }

    assert original_asset_path.exists() != remove_original
    assert bool(_workspace_keys(mock_s3_client, target_bucket, prefix="src/object_asset.tif")) != remove_original

    custom_stac_io = CustomStacIO()

    exported_collection = Collection.from_file(f"s3://{target_bucket}/{merge}", stac_io=custom_stac_io)
    assert exported_collection.id == "collection"
    assert exported_collection.validate_all() == 2

    disk_item, object_item = [item for item in exported_collection.get_items()]

    assert disk_item.id == "disk_asset.tif"
    assert disk_item.get_self_href() == f"s3://{target_bucket}/{merge}/{disk_item.id}/{disk_item.id}.json"
    exported_disk_asset = disk_item.get_assets().pop("disk_asset.tif")
    assert exported_disk_asset.get_absolute_href() == f"s3://{target_bucket}/{merge}/{disk_item.id}/{disk_item.id}"

    assert object_item.id == "object_asset.tif"
    assert object_item.get_self_href() == f"s3://{target_bucket}/{merge}/{object_item.id}/{object_item.id}.json"
    exported_object_asset = object_item.get_assets().pop("object_asset.tif")
    assert (
        exported_object_asset.get_absolute_href() == f"s3://{target_bucket}/{merge}/{object_item.id}/{object_item.id}"
    )

    assert _downloadable_assets(exported_collection, mock_s3_client) == 2


def _collection(
    root_path: Path,
    collection_id: str,
    asset_hrefs: List[str],
    s3_client=None,
    spatial_extent: SpatialExtent = SpatialExtent([[-180, -90, 180, 90]]),
    temporal_extent: TemporalExtent = TemporalExtent([[None, None]]),
) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(spatial_extent, temporal_extent),
    )

    for asset_href in asset_hrefs:
        asset_href_parts = urlparse(asset_href)
        asset_filename = asset_href_parts.path.split("/")[-1]

        item = Item(id=asset_filename, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={})
        asset = Asset(href=asset_href)

        item.add_asset(key=asset_filename, asset=asset)
        collection.add_item(item)

    collection.normalize_and_save(root_href=str(root_path), catalog_type=CatalogType.SELF_CONTAINED)

    assert collection.validate_all() == len(asset_hrefs)
    assert _downloadable_assets(collection, s3_client) == len(asset_hrefs)

    return collection


def _workspace_keys(s3_client, bucket, prefix=None) -> Set[str]:
    kwargs = dict(Bucket=bucket)
    if prefix:
        kwargs["Prefix"] = prefix

    return {obj["Key"] for obj in s3_client.list_objects_v2(**kwargs).get("Contents", [])}


def _downloadable_assets(collection: Collection, s3_client=None) -> int:
    assets = [asset for item in collection.get_items(recursive=True) for asset in item.get_assets().values()]

    for asset in assets:
        url_parts = urlparse(asset.get_absolute_href())

        if url_parts.scheme in ["", "file"]:
            assert Path(url_parts.path).exists()
        elif url_parts.scheme == "s3":
            assert s3_client
            bucket = url_parts.netloc
            key = url_parts.path.lstrip("/")
            assert _workspace_keys(s3_client, bucket, prefix=key)

    return len(assets)
