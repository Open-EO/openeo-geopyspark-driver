import logging
import os
from pathlib import Path, PurePath
from typing import Union
from urllib.parse import urlparse

import botocore.exceptions
from boto3.s3.transfer import TransferConfig

from openeo_driver.utils import remove_slash_prefix
from openeo_driver.workspace import Workspace, _merge_collection_metadata
from pystac import STACObject, Collection, CatalogType, Item, Asset
from pystac.layout import HrefLayoutStrategy, CustomLayoutStrategy

from openeo_driver.integrations.s3.client import S3ClientBuilder
from .custom_stac_io import CustomStacIO
from openeogeotrellis.utils import md5_checksum

_log = logging.getLogger(__name__)


class ObjectStorageWorkspace(Workspace):
    # TODO: support exporting auxiliary files ("_expose_auxiliary")

    MULTIPART_THRESHOLD_IN_MB = 50

    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self.region = region
        self._stac_io = CustomStacIO(region=region)

    def import_file(self, common_path: Union[str, Path], file: Path, merge: str, remove_original: bool = False) -> str:
        merge = os.path.normpath(merge)
        subdirectory = remove_slash_prefix(merge)
        if file.is_absolute():
            file_relative = file.relative_to(common_path)
        else:
            file_relative = file
        file_absolute = Path(common_path) / file_relative

        MB = 1024 ** 2
        config = TransferConfig(multipart_threshold=self.MULTIPART_THRESHOLD_IN_MB * MB)

        key = f"{subdirectory}/{file_relative}"
        S3ClientBuilder.from_region(self.region).upload_file(
            str(file_absolute),
            self.bucket,
            key,
            Config=config,
            ExtraArgs={"Metadata": self._object_metadata(file_absolute)},
        )

        if remove_original:
            file_absolute.unlink()

        workspace_uri = f"s3://{self.bucket}/{key}"

        _log.debug(f"{'moved' if remove_original else 'uploaded'} {file_absolute} to {workspace_uri}")
        return workspace_uri

    def import_object(self, common_path: str, s3_uri: str, merge: str, remove_original: bool = False) -> str:
        uri_parts = urlparse(s3_uri)

        if not uri_parts.scheme or uri_parts.scheme.lower() != "s3":
            raise ValueError(s3_uri)

        source_bucket = uri_parts.netloc
        source_key = remove_slash_prefix(uri_parts.path[1:])
        file_relative = Path(source_key).relative_to(remove_slash_prefix(common_path))

        target_key = f"{merge}/{file_relative}"

        s3 = S3ClientBuilder.from_region(self.region)
        s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key)
        if remove_original:
            s3.delete_object(Bucket=source_bucket, Key=source_key)

        workspace_uri = f"s3://{self.bucket}/{target_key}"

        _log.debug(f"{'moved' if remove_original else 'copied'} s3://{source_bucket}/{source_key} to {workspace_uri}")
        return workspace_uri

    def merge(self, stac_resource: STACObject, target: PurePath, remove_original: bool = False) -> STACObject:
        if not target.suffix:
            # limitation of recent pystac
            raise ValueError("merge argument requires a file extension")

        stac_resource = stac_resource.full_copy()

        # TODO: reduce code duplication with openeo_driver.workspace.DiskWorkspace
        # TODO: relies on item ID == asset key == relative asset path; avoid?
        def href_layout_strategy() -> HrefLayoutStrategy:
            def collection_func(_: Collection, parent_dir: str, is_root: bool) -> str:
                if not is_root:
                    raise NotImplementedError("nested collections")
                # make the collection file end up at $target, not at $target/collection.json
                return f"{parent_dir}/{target.name}"

            def item_func(item: Item, parent_dir: str) -> str:
                unique_item_filename = item.id.replace("/", "_")
                return f"{parent_dir}/{target.name}/{unique_item_filename}.json"

            return CustomLayoutStrategy(collection_func=collection_func, item_func=item_func)

        def replace_asset_href(asset_key: str, asset: Asset, src_collection_path:Path) -> Asset:
            if urlparse(asset.href).scheme not in ["", "file", "s3"]:  # TODO: convenient place; move elsewhere?
                raise ValueError(asset.href)

            # TODO: crummy way to export assets after STAC Collection has been written to disk with new asset hrefs;
            #  it ends up in the asset metadata on disk
            original_absolute_href = asset.href
            asset.extra_fields["_original_absolute_href"] = original_absolute_href
            if original_absolute_href.startswith("s3"):
                asset.href = Path(original_absolute_href).name
            else:
                common_path = os.path.commonpath([original_absolute_href, src_collection_path])
                asset.href = original_absolute_href.replace(common_path + "/","")
            return asset

        if isinstance(stac_resource, Collection):
            new_collection = stac_resource
            new_collection.make_all_asset_hrefs_absolute()

            existing_collection = None
            try:
                existing_collection = Collection.from_file(f"s3://{self.bucket}/{target}", stac_io=self._stac_io)
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "NoSuchKey":
                    raise
            relative_collection_path = Path(new_collection.self_href).parent
            if not existing_collection:
                new_collection.normalize_hrefs(
                    root_href=f"s3://{self.bucket}/{target.parent}", strategy=href_layout_strategy()
                )
                new_collection = new_collection.map_assets(lambda k,v:replace_asset_href(k,v,relative_collection_path))
                new_collection.save(CatalogType.SELF_CONTAINED, stac_io=self._stac_io)

                for new_item in new_collection.get_items():
                    for asset in new_item.assets.values():
                        workspace_uri = self._copy(
                            asset.extra_fields["_original_absolute_href"], new_item.get_self_href(), remove_original, relative_collection_path
                        )
                        asset.extra_fields["alternate"] = {"s3": workspace_uri}
            else:
                merged_collection = _merge_collection_metadata(existing_collection, new_collection)
                new_collection = new_collection.map_assets(lambda k,v:replace_asset_href(k,v,relative_collection_path))

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
                            asset.extra_fields["_original_absolute_href"], new_item.get_self_href(), remove_original, relative_collection_path
                        )
                        asset.extra_fields["alternate"] = {"s3": workspace_uri}

            return new_collection
        else:
            raise NotImplementedError(stac_resource)

    def _copy(self, asset_uri: str, item_s3_uri: str, remove_original: bool, job_dir:Path) -> str:
        source_uri_parts = urlparse(asset_uri)
        source_path = Path(source_uri_parts.path)

        item_uri_parts = urlparse(item_s3_uri)
        target_prefix = "/".join(item_uri_parts.path[1:].split("/")[:-1])
        if asset_uri.startswith("s3"):
            target_name = Path(asset_uri).name
        else:
            common_path =os.path.commonpath([job_dir,asset_uri])
            target_name = asset_uri.replace(common_path + "/","")
        target_key = f"{target_prefix}/{target_name}"

        workspace_uri = f"s3://{self.bucket}/{target_key}"

        if source_uri_parts.scheme in ["", "file"]:
            S3ClientBuilder.from_region(self.region).upload_file(
                str(source_path), self.bucket, target_key, ExtraArgs={"Metadata": self._object_metadata(source_path)}
            )

            if remove_original:
                source_path.unlink()

            _log.debug(f"{'moved' if remove_original else 'uploaded'} {source_path.absolute()} to {workspace_uri}")
        elif source_uri_parts.scheme == "s3":
            source_bucket = source_uri_parts.netloc
            source_key = str(source_path).lstrip("/")

            # TODO: Move away from copy_object because unreliable for cross region
            s3 = S3ClientBuilder.from_region(self.region)
            s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=self.bucket, Key=target_key)
            if remove_original:
                s3.delete_object(Bucket=source_bucket, Key=source_key)

            _log.debug(
                f"{'moved' if remove_original else 'copied'} s3://{source_bucket}/{source_key}" f" to {workspace_uri}"
            )
        else:
            raise ValueError(asset_uri)

        return workspace_uri

    @staticmethod
    def _object_metadata(source_file: Path) -> dict:
        return {
            "md5": md5_checksum(source_file),
            "mtime": str(source_file.stat().st_mtime_ns),
        }
