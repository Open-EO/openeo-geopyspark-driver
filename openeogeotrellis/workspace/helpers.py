import time
from pathlib import PurePath, Path
from typing import Callable
from urllib.parse import urlparse

import pystac
from openeo_driver.util.auth import _AccessTokenCache
from openeo.rest.auth.oidc import OidcClientCredentialsAuthenticator, OidcClientInfo, OidcProviderInfo

from .stac_api_workspace import StacApiWorkspace
from openeogeotrellis.utils import s3_client, md5_checksum


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
            s3_client().upload_file(
                str(source_path),
                asset_bucket,
                target_key,
                ExtraArgs={
                    "Metadata": {
                        "md5": md5_checksum(source_path),
                        "mtime": str(source_path.stat().st_mtime_ns),
                    }
                },
            )

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

    return StacApiWorkspace(
        root_url=root_url,
        export_asset=export_asset,
        asset_alternate_id="s3",
        additional_collection_properties=additional_collection_properties,
        get_access_token=get_oidc_access_token(oidc_issuer, oidc_client_id, oidc_client_secret),
    )


def get_oidc_access_token(
    oidc_issuer: str, oidc_client_id: str, oidc_client_secret
) -> Callable[[bool], str]:  # fresh => access_token
    access_token_cache = _AccessTokenCache(access_token="", expires_at=0)

    def get_access_token(fresh: bool) -> str:
        nonlocal access_token_cache

        if fresh or time.time() > access_token_cache.expires_at:
            access_token = _fetch_access_token()
            # TODO: get expiry from access token itself?
            access_token_cache = _AccessTokenCache(access_token, time.time() + 5 * 60)
        return access_token_cache.access_token

    def _fetch_access_token() -> str:
        session = None  # TODO: use Session with retries

        oidc_provider = OidcProviderInfo(
            issuer=oidc_issuer,
            requests_session=None,
        )

        client_info = OidcClientInfo(
            client_id=oidc_client_id,
            provider=oidc_provider,
            client_secret=oidc_client_secret,
        )

        authenticator = OidcClientCredentialsAuthenticator(
            client_info,
            session,
        )

        return authenticator.get_tokens().access_token

    return get_access_token
