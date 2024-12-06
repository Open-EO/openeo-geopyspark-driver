import logging
import os
from pathlib import Path, PurePath
from typing import Union, Any
from urllib.parse import urlparse

import boto3
import botocore.exceptions
from boto3.s3.transfer import TransferConfig

from openeo_driver.utils import remove_slash_prefix
from openeo_driver.workspace import Workspace, _merge_collection_metadata
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

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        merge = os.path.normpath(merge)
        subdirectory = remove_slash_prefix(merge)
        file_relative = file.relative_to(common_path)

        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * MB)

        key = f"{subdirectory}/{file_relative}"
        s3_client().upload_file(str(file), self.bucket, key, Config=config)

        if remove_original:
            file.unlink()

        workspace_uri = f"s3://{self.bucket}/{key}"

        _log.debug(f"{'moved' if remove_original else 'uploaded'} {file.absolute()} to {workspace_uri}")
        return workspace_uri

    def import_object(self, common_path: str, s3_uri: str, merge: str, remove_original: bool = False) -> str:
        uri_parts = urlparse(s3_uri)

        if not uri_parts.scheme or uri_parts.scheme.lower() != "s3":
            raise ValueError(s3_uri)

        source_bucket = uri_parts.netloc
        source_key = remove_slash_prefix(uri_parts.path[1:])
        file_relative = Path(source_key).relative_to(remove_slash_prefix(common_path))

        target_key = f"{merge}/{file_relative}"

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
        # TODO: relies on item ID == asset key == asset path; avoid?
        def href_layout_strategy() -> HrefLayoutStrategy:
            def collection_func(_: Collection, parent_dir: str, is_root: bool) -> str:
                if not is_root:
                    raise NotImplementedError("nested collections")
                # make the collection file end up at $target, not at $target/collection.json
                return f"{parent_dir}/{target.name}"

            def item_func(item: Item, parent_dir: str) -> str:
                return f"{parent_dir}/{target.name}/{item.id}.json"  # item ID == asset key == relative asset path

            return CustomLayoutStrategy(collection_func=collection_func, item_func=item_func)

        def replace_asset_href(asset_key: str, asset: Asset) -> Asset:
            if urlparse(asset.href).scheme not in ["", "file", "s3"]:  # TODO: convenient place; move elsewhere?
                raise ValueError(asset.href)

            # TODO: crummy way to export assets after STAC Collection has been written to disk with new asset hrefs;
            #  it ends up in the asset metadata on disk
            asset.extra_fields["_original_absolute_href"] = asset.get_absolute_href()
            asset.href = Path(asset_key).name  # asset key == relative asset path, including subdirectories
            return asset

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource

            existing_collection = None
            try:
                existing_collection = Collection.from_file(f"s3://{self.bucket}/{target}", stac_io=self._stac_io)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise

            if not existing_collection:
                new_collection.normalize_hrefs(
                    root_href=f"s3://{self.bucket}/{target.parent}", strategy=href_layout_strategy()
                )
                new_collection = new_collection.map_assets(replace_asset_href)
                new_collection.save(CatalogType.SELF_CONTAINED, stac_io=self._stac_io)

                for new_item in new_collection.get_items():
                    for asset in new_item.assets.values():
                        workspace_uri = self._copy(
                            asset.extra_fields["_original_absolute_href"], new_item.get_self_href(), remove_original
                        )
                        asset.extra_fields["alternate"] = {"s3": workspace_uri}

                merged_collection = new_collection
            else:
                merged_collection = _merge_collection_metadata(existing_collection, new_collection)
                new_collection = new_collection.map_assets(replace_asset_href)

                for new_item in new_collection.get_items():
                    new_item.clear_links()  # sever ties with previous collection
                    merged_collection.add_item(new_item, strategy=href_layout_strategy())

                merged_collection.normalize_hrefs(
                    root_href=f"s3://{self.bucket}/{target.parent}", strategy=href_layout_strategy()
                )
                merged_collection.save(CatalogType.SELF_CONTAINED)

                for new_item in new_collection.get_items():
                    for asset in new_item.assets.values():
                        workspace_uri = self._copy(
                            asset.extra_fields["_original_absolute_href"], new_item.get_self_href(), remove_original
                        )
                        asset.extra_fields["alternate"] = {"s3": workspace_uri}

            return merged_collection
        else:
            raise NotImplementedError(stac_resource)

    def _copy(self, asset_uri: str, item_s3_uri: str, remove_original: bool) -> str:
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

        return f"s3://{self.bucket}/{target_key}"


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
