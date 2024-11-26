import datetime as dt
import shutil
import tempfile
from pathlib import PurePath, Path
from typing import Set

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


def test_merge_new(mock_s3_client, mock_s3_bucket, tmp_path):
    target_bucket = "openeo-fake-bucketname"
    merge = PurePath("some/target/collection")

    new_collection = _collection(
        root_path=tmp_path / "src" / "collection", collection_id="collection", asset_filename="asset.tif"
    )

    workspace = ObjectStorageWorkspace(bucket=target_bucket)
    workspace.merge(new_collection, merge)  # TODO: check returned workspace URI

    assert _workspace_keys(mock_s3_client, target_bucket) == {
        "some/target/collection",
        "some/target/collection/asset.tif/asset.tif.json",
        "some/target/collection/asset.tif/asset.tif",
    }

    custom_stac_io = CustomStacIO()

    exported_collection = Collection.from_file(f"s3://{target_bucket}/{merge}", stac_io=custom_stac_io)
    assert exported_collection.id == "collection"
    assert exported_collection.validate_all() == 1

    item = [item for item in exported_collection.get_items()][0]
    assert item.id == "asset.tif"
    assert item.get_self_href() == f"s3://{target_bucket}/{merge}/{item.id}/{item.id}.json"

    asset = item.get_assets().pop("asset.tif")
    assert asset.get_absolute_href() == f"s3://{target_bucket}/{merge}/{item.id}/{item.id}"

    assert custom_stac_io.read_text(asset.get_absolute_href()) == "asset.tif\n"


def _collection(
    root_path: Path,
    collection_id: str,
    asset_filename: str,
    spatial_extent: SpatialExtent = SpatialExtent([[-180, -90, 180, 90]]),
    temporal_extent: TemporalExtent = TemporalExtent([[None, None]]),
) -> Collection:
    collection = Collection(
        id=collection_id,
        description=collection_id,
        extent=Extent(spatial_extent, temporal_extent),
    )

    item = Item(id=asset_filename, geometry=None, bbox=None, datetime=dt.datetime.utcnow(), properties={})

    asset_path = root_path / item.id / asset_filename
    asset = Asset(href=asset_path.name)  # relative to item

    item.add_asset(key=asset_filename, asset=asset)
    collection.add_item(item)

    collection.normalize_and_save(root_href=str(root_path), catalog_type=CatalogType.SELF_CONTAINED)

    with open(asset_path, "w") as f:
        f.write(f"{asset_filename}\n")

    assert collection.validate_all() == 1
    assert _downloadable_assets(collection) == 1

    return collection


def _workspace_keys(s3_client, bucket, prefix=None) -> Set[str]:
    kwargs = dict(Bucket=bucket)
    if prefix:
        kwargs["Prefix"] = prefix

    return {obj["Key"] for obj in s3_client.list_objects_v2(**kwargs).get("Contents", [])}


def _downloadable_assets(collection: Collection) -> int:
    assets = [asset for item in collection.get_items(recursive=True) for asset in item.get_assets().values()]

    for asset in assets:
        with tempfile.NamedTemporaryFile(mode="wb") as temp_file:
            shutil.copy(asset.get_absolute_href(), temp_file.name)  # "download" the asset without altering its href

    return len(assets)
