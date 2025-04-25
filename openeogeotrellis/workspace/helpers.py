from pathlib import PurePath, Path
from typing import Callable
from urllib.parse import urlparse

import pystac
from openeo_driver.util.auth import ClientCredentialsAccessTokenHelper, ClientCredentials

from .stac_api_workspace import StacApiWorkspace
from openeogeotrellis.utils import s3_client


def vito_stac_api_workspace(  # for lack of a better name, can still be aliased
    root_url: str,
    oidc_issuer: str,
    oidc_client_id: str,
    oidc_client_secret: str,
    asset_bucket: str,
    asset_prefix: Callable[[PurePath], str] = lambda merge: str(merge).lstrip("/"),
    additional_collection_properties: dict = None,
) -> StacApiWorkspace:
    """
    Returns a ``StacApiWorkspace`` that:
    - uses OIDC client credentials to obtain an access token for the STAC API;
    - exports assets to a particular bucket/prefix in object storage.

    :param root_url: root URL of the STAC API
    :param oidc_issuer: URL of the OIDC issuer
    :param oidc_client_id: OIDC client ID
    :param oidc_client_secret: OIDC client secret
    :param asset_bucket: name of the object storage bucket to export assets to
    :param asset_prefix: customize the object storage prefix assets are stored at; defaults to the ``merge`` argument passed to ``export_workspace``.
    :param additional_collection_properties: top-level Collection properties to include in the STAC API request for e.g. authorization
    """

    def export_asset(asset: pystac.Asset, merge: PurePath, relative_asset_path: PurePath, remove_original: bool) -> str:
        asset_uri = asset.get_absolute_href()
        source_uri_parts = urlparse(asset_uri)
        source_path = Path(source_uri_parts.path)

        target_prefix = asset_prefix(merge)
        target_key = f"{target_prefix}/{relative_asset_path}"

        if source_uri_parts.scheme in ["", "file"]:
            s3_client().upload_file(str(source_path), asset_bucket, target_key)

            if remove_original:
                source_path.unlink()
        elif source_uri_parts.scheme == "s3":
            source_bucket = source_uri_parts.netloc
            source_key = str(source_path).lstrip("/")

            s3 = s3_client()
            s3.copy_object(CopySource={"Bucket": source_bucket, "Key": source_key}, Bucket=asset_bucket, Key=target_key)
            if remove_original:
                s3.delete_object(Bucket=source_bucket, Key=source_key)
        else:
            raise ValueError(asset_uri)

        workspace_uri = f"s3://{asset_bucket}/{target_key}"
        return workspace_uri

    def get_access_token() -> str:
        # intentionally lazily instantiated
        access_token_helper = ClientCredentialsAccessTokenHelper(
            credentials=ClientCredentials(
                oidc_issuer=oidc_issuer,
                client_id=oidc_client_id,
                client_secret=oidc_client_secret,
            ),
        )

        return access_token_helper.get_access_token()

    return StacApiWorkspace(
        root_url=root_url,
        export_asset=export_asset,
        asset_alternate_id="s3",
        additional_collection_properties=additional_collection_properties,
        get_access_token=get_access_token,
    )
