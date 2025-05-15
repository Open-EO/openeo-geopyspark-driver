import datetime as dt
from pathlib import PurePath, Path
from typing import Set, List
from urllib.parse import urlparse

from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Item, CatalogType, Asset
import pytest

from openeo_driver.util.date_math import now_utc
from openeogeotrellis.workspace import ObjectStorageWorkspace
from openeogeotrellis.workspace.custom_stac_io import CustomStacIO


@pytest.mark.parametrize("remove_original", [False, True])
def test_import_file(tmp_path, mock_s3_client, mock_s3_bucket, remove_original):
    source_directory = tmp_path / "src"
    source_directory.mkdir()
    source_file = source_directory / "file"
    source_file.touch()

    merge = "some/target"

    workspace = ObjectStorageWorkspace(bucket="openeo-fake-bucketname", region="waw3-1")
    workspace_uri = workspace.import_file(
        common_path=source_directory, file=source_file, merge=merge, remove_original=remove_original
    )

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

    workspace = ObjectStorageWorkspace(bucket=target_bucket, region="eu-nl")
    workspace_uri = workspace.import_object(
        common_path="some/source/",
        s3_uri=f"s3://{source_bucket}/{source_key}",
        merge=merge,
        remove_original=remove_original,
    )

    assert workspace_uri == f"s3://{target_bucket}/some/target/object"
    assert _workspace_keys(mock_s3_client, workspace.bucket, prefix=merge) == {"some/target/object"}

    if remove_original:
        assert _workspace_keys(mock_s3_client, source_bucket) == {"some/target/object"}
    else:
        assert _workspace_keys(mock_s3_client, source_bucket) == {source_key, "some/target/object"}


@pytest.mark.parametrize("remove_original", [False, True])
def test_merge_new(mock_s3_client, mock_s3_bucket, tmp_path, remove_original: bool):
    test_region = "eu-nl"
    merge = PurePath("some") / "target" / "collection"

    source_directory = tmp_path / "src"
    source_directory.mkdir()
    disk_asset_path = source_directory / "disk_asset.tif"

    with open(disk_asset_path, "w") as f:
        f.write("disk_asset.tif\n")

    source_bucket = target_bucket = "openeo-fake-bucketname"
    source_key = "src/object_asset.tif"

    mock_s3_client.put_object(Bucket=source_bucket, Key=source_key, Body="object_asset.tif\n")

    new_collection = _collection(
        root_path=source_directory / "new_collection",
        collection_id="new_collection",
        asset_hrefs=[str(disk_asset_path), f"s3://{source_bucket}/{source_key}"],
        s3_client=mock_s3_client,
    )

    workspace = ObjectStorageWorkspace(bucket=target_bucket, region=test_region)
    imported_collection = workspace.merge(new_collection, merge, remove_original)

    assert isinstance(imported_collection, Collection)

    asset_workspace_uris = {
        asset_key: asset.extra_fields["alternate"]["s3"]
        for item in imported_collection.get_items()
        for asset_key, asset in item.get_assets().items()
    }

    assert asset_workspace_uris == {
        "disk_asset.tif": f"s3://{target_bucket}/{merge}/disk_asset.tif",
        "object_asset.tif": f"s3://{target_bucket}/{merge}/object_asset.tif",
    }

    assert _workspace_keys(mock_s3_client, target_bucket, prefix="some/target/collection") == {
        "some/target/collection",
        "some/target/collection/disk_asset.tif.json",
        "some/target/collection/disk_asset.tif",
        "some/target/collection/object_asset.tif.json",
        "some/target/collection/object_asset.tif",
    }

    assert disk_asset_path.exists() != remove_original
    assert bool(_workspace_keys(mock_s3_client, workspace.bucket, prefix=source_key)) != remove_original

    exported_collection = Collection.from_file(f"s3://{workspace.bucket}/{merge}", stac_io=CustomStacIO(test_region))
    assert exported_collection.validate_all() == 2

    disk_item, object_item = [item for item in exported_collection.get_items()]

    assert disk_item.id == "disk_asset.tif"
    assert disk_item.collection_id == exported_collection.id
    assert disk_item.get_self_href() == f"s3://{workspace.bucket}/{merge}/{disk_item.id}.json"
    exported_disk_asset = disk_item.get_assets().pop("disk_asset.tif")
    assert exported_disk_asset.get_absolute_href() == f"s3://{workspace.bucket}/{merge}/{disk_item.id}"

    assert object_item.id == "object_asset.tif"
    assert object_item.collection_id == exported_collection.id
    assert object_item.get_self_href() == f"s3://{workspace.bucket}/{merge}/{object_item.id}.json"
    exported_object_asset = object_item.get_assets().pop("object_asset.tif")
    assert exported_object_asset.get_absolute_href() == f"s3://{workspace.bucket}/{merge}/{object_item.id}"

    assert _downloadable_assets(exported_collection, mock_s3_client) == 2


@pytest.mark.parametrize("remove_original", [False, True])
def test_merge_into_existing(tmp_path, mock_s3_client, mock_s3_bucket, remove_original):
    test_region = "waw3-1"
    merge = PurePath("some") / "target" / "collection"

    source_directory = tmp_path / "src"
    source_directory.mkdir()
    disk_asset_path = source_directory / "disk_asset.tif"

    with open(disk_asset_path, "w") as f:
        f.write("disk_asset.tif\n")

    source_bucket = target_bucket = "openeo-fake-bucketname"
    source_key = "src/object_asset.tif"

    mock_s3_client.put_object(Bucket=source_bucket, Key=source_key, Body="object_asset.tif\n")

    existing_collection = _collection(
        root_path=tmp_path / "src" / "existing_collection",
        collection_id="existing_collection",
        asset_hrefs=[str(disk_asset_path)],
        spatial_extent=SpatialExtent([[0, 50, 2, 52]]),
        temporal_extent=TemporalExtent([[
            dt.datetime.fromisoformat("2024-11-01T00:00:00+00:00"),
            dt.datetime.fromisoformat("2024-11-03T00:00:00+00:00")
        ]]),
    )

    new_collection = _collection(
        root_path=tmp_path / "src" / "new_collection",
        collection_id="new_collection",
        asset_hrefs=[f"s3://{source_bucket}/{source_key}"],
        spatial_extent=SpatialExtent([[1, 51, 3, 53]]),
        temporal_extent=TemporalExtent([[
            dt.datetime.fromisoformat("2024-11-02T00:00:00+00:00"),
            dt.datetime.fromisoformat("2024-11-04T00:00:00+00:00")
        ]]),
        s3_client=mock_s3_client,
    )

    workspace = ObjectStorageWorkspace(target_bucket, region=test_region)
    workspace.merge(existing_collection, target=merge, remove_original=remove_original)
    imported_collection = workspace.merge(new_collection, target=merge, remove_original=remove_original)

    assert isinstance(imported_collection, Collection)

    asset_workspace_uris = {
        asset_key: asset.extra_fields["alternate"]["s3"]
        for item in imported_collection.get_items()
        for asset_key, asset in item.get_assets().items()
    }

    assert asset_workspace_uris == {
        "object_asset.tif": f"s3://{target_bucket}/{merge}/object_asset.tif",
    }

    assert _workspace_keys(mock_s3_client, target_bucket, prefix="some/target/collection") == {
        "some/target/collection",
        "some/target/collection/disk_asset.tif",
        "some/target/collection/disk_asset.tif.json",
        "some/target/collection/object_asset.tif",
        "some/target/collection/object_asset.tif.json",
    }

    assert disk_asset_path.exists() != remove_original
    assert bool(_workspace_keys(mock_s3_client, target_bucket, prefix=source_key)) != remove_original

    exported_collection = Collection.from_file(f"s3://{workspace.bucket}/{merge}", stac_io=CustomStacIO(test_region))

    assert exported_collection.extent.spatial.bboxes == [[0, 50, 3, 53]]
    assert exported_collection.extent.temporal.intervals == [[
        dt.datetime.fromisoformat("2024-11-01T00:00:00+00:00"),
        dt.datetime.fromisoformat("2024-11-04T00:00:00+00:00")
    ]]

    assert exported_collection.validate_all() == 2

    items = [item for item in exported_collection.get_items()]
    assert len(items) == 2
    assert all(item.collection_id == exported_collection.id for item in items)

    assert _downloadable_assets(exported_collection, mock_s3_client) == 2


def _collection(
    root_path: Path,
    collection_id: str,
    asset_hrefs: List[str] = None,
    spatial_extent: SpatialExtent = SpatialExtent([[-180, -90, 180, 90]]),
    temporal_extent: TemporalExtent = TemporalExtent([[None, None]]),
    s3_client=None,
) -> Collection:
    if asset_hrefs is None:
        asset_hrefs = []

    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(spatial_extent, temporal_extent),
    )

    for asset_href in asset_hrefs:
        asset_href_parts = urlparse(asset_href)
        asset_filename = asset_href_parts.path.split("/")[-1]

        item = Item(id=asset_filename, geometry=None, bbox=None, datetime=now_utc(), properties={})
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
