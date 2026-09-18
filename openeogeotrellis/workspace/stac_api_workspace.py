from pathlib import PurePath
from typing import Callable

from pystac import Asset, Link

from openeogeotrellis.integrations.stac import LoggingStacApiIO, ResilientStacIO
from openeogeotrellis.job_results.workspaces.stac_api_workspace import StacApiWorkspace as _StacApiWorkspace
from openeogeotrellis.job_results.workspaces.stac_api_workspace import StacApiResponseError  # noqa: F401 (public re-export)


class StacApiWorkspace(_StacApiWorkspace):
    def __init__(
        self,
        root_url: str,
        export_asset: Callable[
            [Asset, PurePath, PurePath, bool],  # (asset, merge, relative_asset_path, remove_original)
            str,  #  => workspace URI
        ],
        asset_alternate_id: str,
        export_link: Callable[
            [Link, PurePath, bool], str  # (link, merge, remove_original) => workspace URI
        ] = lambda link, merge, remove_original: link.href,
        additional_collection_properties: dict = None,
        get_access_token: Callable[[bool], str] = None,  # fresh => access_token
    ):
        """
        :param root_url: the URL to the STAC API's root catalog
        :param additional_collection_properties: top-level Collection properties to include in the request
        :param get_access_token: supply an access token, if needed
        :param export_asset: copy/move an asset and return its workspace URI, to be used as an alternate URI
        :param asset_alternate_id
        """
        super().__init__(
            root_url=root_url,
            export_asset=export_asset,
            asset_alternate_id=asset_alternate_id,
            resilient_stac_io_class=ResilientStacIO,
            logging_stac_api_io_class=LoggingStacApiIO,
            export_link=export_link,
            additional_collection_properties=additional_collection_properties,
            get_access_token=get_access_token,
        )
