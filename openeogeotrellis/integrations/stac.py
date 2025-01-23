import json
import logging
import warnings
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Tuple,
    Union, List, Callable,
)

import pystac
import pystac_client
from pystac.serialization import (
    identify_stac_object,
    identify_stac_object_type,
    merge_common_properties,
    migrate_to_latest,
)
from pystac_client.exceptions import APIError
from pystac_client.stac_api_io import StacApiIO
from requests import Request, Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

if TYPE_CHECKING:
    from pystac.catalog import Catalog as Catalog_Type
    from pystac.stac_object import STACObject as STACObject_Type

logger = logging.getLogger(__name__)


Timeout = Union[float, Tuple[float, float], Tuple[float, None]]


class StacApiIO(StacApiIO):

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        conformance: Optional[List[str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
        request_modifier: Optional[Callable[[Request], Union[Request, None]]] = None,
        timeout: Optional[Timeout] = None,
        max_retries: Optional[Union[int, Retry]] = 5,
    ):
        """Initialize class for API IO

        Args:
            headers : Optional dictionary of headers to include in all requests
            conformance (DEPRECATED) : Optional list of `Conformance Classes
                <https://github.com/radiantearth/stac-api-spec/blob/master/overview.md#conformance-classes>`__.

                .. deprecated:: 0.7.0
                    Conformance can be altered on the client class directly

            parameters: Optional dictionary of query string parameters to
              include in all requests.
            request_modifier: Optional callable that can be used to modify Request
              objects before they are sent. If provided, the callable receives a
              `request.Request` and must either modify the object directly or return
              a new / modified request instance.
            timeout: Optional float or (float, float) tuple following the semantics
              defined by `Requests
              <https://requests.readthedocs.io/en/latest/api/#main-interface>`__.
            max_retries: The number of times to retry requests. Set to ``None`` to
              disable retries.

        Return:
            StacApiIO : StacApiIO instance
        """
        if conformance is not None:
            warnings.warn(
                (
                    "The `conformance` option is deprecated and will be "
                    "removed in the next major release. Instead use "
                    "`Client.set_conforms_to` or `Client.add_conforms_to` to control "
                    "behavior."
                ),
                category=FutureWarning,
            )

        self.session = Session()
        if max_retries:
            self.session.mount("http://", HTTPAdapter(max_retries=max_retries))
            self.session.mount("https://", HTTPAdapter(max_retries=max_retries))
        self.update(
            headers=headers, parameters=parameters, request_modifier=request_modifier, timeout=timeout
        )

    def request(
        self,
        href: str,
        method: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Makes a request to an http endpoint

        Args:
            href (str): The request URL
            method (Optional[str], optional): The http method to use, 'GET' or 'POST'.
              Defaults to None, which will result in 'GET' being used.
            headers (Optional[Dict[str, str]], optional): Additional headers to include
                in request. Defaults to None.
            parameters (Optional[Dict[str, Any]], optional): parameters to send with
                request. Defaults to None.

        Raises:
            APIError: raised if the server returns an error response

        Return:
            str: The decoded response from the endpoint
        """
        if method == "POST":
            request = Request(method=method, url=href, headers=headers, json=parameters)
        else:
            params = deepcopy(parameters) or {}
            request = Request(method="GET", url=href, headers=headers, params=params)
        try:
            modified = self._req_modifier(request) if self._req_modifier else None
            prepped = self.session.prepare_request(modified or request)
            msg = f"{prepped.method} {prepped.url} Headers: {prepped.headers}"
            if method == "POST":
                msg += f" Payload: {json.dumps(request.json)}"
            if self.timeout is not None:
                msg += f" Timeout: {self.timeout}"
            logger.debug(msg)
            # The only difference with the super implementation are these extra send kwargs.
            send_kwargs = self.session.merge_environment_settings(
                prepped.url, proxies={}, stream=None, verify=True, cert=None
            )
            resp = self.session.send(prepped, timeout=self.timeout, **send_kwargs)
        except Exception as err:
            logger.debug(err)
            raise APIError(str(err))
        if resp.status_code != 200:
            raise APIError.from_response(resp)
        try:
            return resp.content.decode("utf-8")
        except Exception as err:
            raise APIError(str(err))

    def stac_object_from_dict(
        self,
        d: Dict[str, Any],
        href: Optional[pystac.link.HREF] = None,
        root: Optional["Catalog_Type"] = None,
        preserve_dict: bool = True,
    ) -> "STACObject_Type":
        """Deserializes a :class:`~pystac.STACObject` sub-class instance from a
        dictionary.

        Args:
            d : The dictionary to deserialize
            href : Optional href to associate with the STAC object
            root : Optional root :class:`~pystac.Catalog` to associate with the
                STAC object.
            preserve_dict: If ``False``, the dict parameter ``d`` may be modified
                during this method call. Otherwise the dict is not mutated.
                Defaults to ``True``, which results results in a deepcopy of the
                parameter. Set to ``False`` when possible to avoid the performance
                hit of a deepcopy.
        """
        if identify_stac_object_type(d) == pystac.STACObjectType.ITEM:
            collection_cache = None
            if root is not None:
                collection_cache = root._resolved_objects.as_collection_cache()

            # Merge common properties in case this is an older STAC object.
            merge_common_properties(
                d, json_href=str(href), collection_cache=collection_cache
            )

        info = identify_stac_object(d)
        d = migrate_to_latest(d, info)

        if info.object_type == pystac.STACObjectType.CATALOG:
            result = pystac_client.client.Client.from_dict(
                d, href=str(href), root=root, migrate=False, preserve_dict=preserve_dict
            )
            result._stac_io = self
            return result

        if info.object_type == pystac.STACObjectType.COLLECTION:
            collection_client = (
                pystac_client.collection_client.CollectionClient.from_dict(
                    d,
                    href=str(href),
                    root=root,
                    migrate=False,
                    preserve_dict=preserve_dict,
                )
            )
            # The only difference with the super implementation is that we set _stac_io here.
            # This ensures root_link.resolve_stac_object() uses this StacApiIO and not the default one.
            # Which in turn makes sure that the Catalog is always of type pystac_client.client.Client.
            collection_client._stac_io = self
            return collection_client

        if info.object_type == pystac.STACObjectType.ITEM:
            return pystac.Item.from_dict(
                d, href=str(href), root=root, migrate=False, preserve_dict=preserve_dict
            )

        raise ValueError(f"Unknown STAC object type {info.object_type}")
