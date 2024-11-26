import logging
import os
from pathlib import Path, PurePath
from typing import Union, Any
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig

from openeo_driver.workspace import Workspace
from pystac import STACObject, Collection, CatalogType, Link, Item, Asset
from pystac.layout import HrefLayoutStrategy, CustomLayoutStrategy
from pystac.stac_io import DefaultStacIO

from openeogeotrellis.utils import s3_client

_log = logging.getLogger(__name__)


class ObjectStorageWorkspace(Workspace):

    MULTIPART_THRESHOLD_IN_MB = 50

    def __init__(self, bucket: str):
        self.bucket = bucket
        self._stac_io = CustomStacIO()

    def import_file(self, file: Path, merge: str, remove_original: bool = False) -> str:
        merge = os.path.normpath(merge)
        subdirectory = merge[1:] if merge.startswith("/") else merge

        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * MB)

        key = subdirectory + "/" + file.name
        s3_client().upload_file(str(file), self.bucket, key, Config=config)

        if remove_original:
            file.unlink()

        workspace_uri = f"s3://{self.bucket}/{key}"

        _log.debug(f"{'moved' if remove_original else 'uploaded'} {file.absolute()} to {workspace_uri}")
        return workspace_uri

    def import_object(self, s3_uri: str, merge: str, remove_original: bool = False) -> str:
        uri_parts = urlparse(s3_uri)

        if not uri_parts.scheme or uri_parts.scheme.lower() != "s3":
            raise ValueError(s3_uri)

        source_bucket = uri_parts.netloc
        source_key = uri_parts.path[1:]
        filename = source_key.split("/")[-1]

        target_key = f"{merge}/{filename}"

        s3 = s3_client()
        s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key)
        if remove_original:
            s3.delete_object(Bucket=source_bucket, Key=source_key)

        workspace_uri = f"s3://{self.bucket}/{target_key}"

        _log.debug(f"{'moved' if remove_original else 'copied'} s3://{source_bucket}/{source_key} to {workspace_uri}")
        return workspace_uri

    def merge(self, stac_resource: STACObject, target: PurePath, remove_original: bool = False) -> STACObject:
        # Originally, STAC objects came from files on disk and assets could either be on disk or in S3; this is
        # reflected in the import_file and import_object methods.

        # Now, we have a STACObject in memory that can be saved to S3 as well as assets that can be on disk or in S3.
        # The former requires a pystac.StacIO implementation: https://pystac.readthedocs.io/en/stable/concepts.html#i-o-in-pystac
        # The latter should check each asset href's scheme and act accordingly: upload or copy.
        stac_resource = stac_resource.full_copy()

        # TODO: reduce code duplication with openeo_driver.workspace.DiskWorkspace
        def href_layout_strategy() -> HrefLayoutStrategy:
            def collection_func(_: Collection, parent_dir: str, is_root: bool) -> str:
                if not is_root:
                    raise NotImplementedError("nested collections")
                # make the collection file end up at $target, not at $target/collection.json
                return parent_dir

            def item_func(item: Item, parent_dir: str) -> str:
                return f"{parent_dir}/{item.collection_id}/{item.id}/{item.id}.json"

            return CustomLayoutStrategy(collection_func=collection_func, item_func=item_func)

        def replace_asset_href(asset_key: str, asset: Asset) -> Asset:
            if urlparse(asset.href).scheme not in ["", "file", "s3"]:  # TODO: convenient place; move elsewhere?
                raise ValueError(asset.href)

            # TODO: crummy way to export assets after STAC Collection has been written to disk with new asset hrefs;
            #  it ends up in the asset metadata on disk
            asset.extra_fields["_original_absolute_href"] = asset.get_absolute_href()
            asset.href = asset_key  # asset key matches the asset filename, becomes the relative path
            return asset

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource

            new_collection.normalize_hrefs(root_href=f"s3://{self.bucket}/{target}", strategy=href_layout_strategy())
            new_collection = new_collection.map_assets(replace_asset_href)
            new_collection.save(CatalogType.SELF_CONTAINED, stac_io=self._stac_io)

            for item in new_collection.get_items():
                for asset in item.get_assets().values():
                    self._copy(asset.extra_fields["_original_absolute_href"], item.get_self_href(), remove_original)

            # TODO: support merge into existing
            return new_collection
        else:
            raise NotImplementedError(stac_resource)

    def _copy(self, asset_uri: str, item_s3_uri: str, remove_original: bool):
        source_uri_parts = urlparse(asset_uri)
        source_path = Path(source_uri_parts.path)

        item_uri_parts = urlparse(item_s3_uri)
        target_prefix = "/".join(item_uri_parts.path[1:].split("/")[:-1])
        target_key = f"{target_prefix}/{source_path.name}"

        if source_uri_parts.scheme in ["", "file"]:
            s3_client().upload_file(source_path, self.bucket, target_key)

            if remove_original:
                source_path.unlink()
        elif source_uri_parts.scheme == "s3":
            source_bucket = source_uri_parts.netloc
            source_key = str(source_path).lstrip("/")

            s3 = s3_client()
            s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key)
            if remove_original:
                s3.delete_object(Bucket=source_bucket, Key=source_key)
        else:
            raise ValueError(asset_uri)


# TODO: move to dedicated file?
class CustomStacIO(DefaultStacIO):  # supports S3 as well

    @property
    def _s3(self):
        # TODO: otherwise there's an infinite recursion error upon Item.get_assets() wrt/ some boto3 reference
        return boto3.resource("s3")

    def read_text(self, source: Union[str, Link], *args: Any, **kwargs: Any) -> str:
        parsed = urlparse(source)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]

            obj = self._s3.Object(bucket, key)
            return obj.get()["Body"].read().decode("utf-8")
        else:
            return super().read_text(source, *args, **kwargs)

    def write_text(self, dest: Union[str, Link], txt: str, *args: Any, **kwargs: Any) -> None:
        parsed = urlparse(dest)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            self._s3.Object(bucket, key).put(Body=txt, ContentEncoding="utf-8")
        else:
            super().write_text(dest, txt, *args, **kwargs)
