"""
Integration with FreeIPA (an Identity, Policy and Audit system) for user management
(e.g. as used on VITO/Terrascope YARN cluster).

There is an official FreeIPA Python library, but it's quite complex and dependency-heavy.
Essentially it's just doing HTTP requests, which shouldn't be hard to do ourselves,
as we just need to cover a tiny subset of the FreeIPA API.

Apparently it's hard to find useful, official documentation of the JSON-RPC API of FreeIPA,
but here are some references that might help:

- Very little API information to find on the official FreeIPA website:
  https://www.freeipa.org/page/Documentation
- This unofficial(?) documentation seems to be the more useful:
  https://freeipa.readthedocs.io/en/latest/api/index.html
  for example with an extensive overview of all FreeIPA API commands with their arguments/options:
  https://freeipa.readthedocs.io/en/latest/api/commands.html
- Practical blog post on JSON-RPC requests to FreeIPA:
  https://vda.li/en/posts/2015/05/28/talking-to-freeipa-api-with-sessions/

"""
from __future__ import annotations

import configparser
import logging
import os
from pathlib import Path
from typing import Union, Optional, Iterable, Mapping

import requests
import sys

from openeo_driver.utils import generate_unique_id, smart_bool

try:
    # Import of optional gssapi packages
    import gssapi
    import requests_gssapi
except ImportError:
    requests_gssapi = None

_log = logging.getLogger(__name__)


class FreeIpaException(Exception):
    """Base class for FreeIPA exceptions"""


class FreeIpaRpcException(FreeIpaException):
    """Exceptions resulting from a non-empty 'error' field in JSON RPC response"""


class FreeIpaRpcResponse:
    """Container for JSON-RPC response fields, mainly 'result' and 'error'"""

    __slots__ = ("result", "error")

    def __init__(self, result: Union[dict, None], error: Union[None, dict]):
        self.result = result
        self.error = error

    def raise_on_error(self):
        if self.error:
            code, name, message = (self.error.get(k) for k in ["code", "name", "message"])
            raise FreeIpaRpcException(f"{code} ({name}): {message!r}")


class FreeIpaClient:
    def __init__(
        self,
        ipa_server: str,
        verify_tls: Union[bool, str, Path] = True,
        api_version: str = "2.231",
        gssapi_creds: Optional["gssapi.Credentials"] = None,
        opportunistic_auth: bool = True,
    ):
        """
        :param ipa_server: ipa server (hostname)
        :param verify_tls: whether/how to verify TLS certificates:
            as a boolean to enable (default) or disable it;
            or as a path to a CA certificate file to verify against (e.g. `/etc/ipa/ca.crt`).
        :param api_version:
        :param gssapi_creds: Optional GSSAPI credentials to use for authentication
            By default (given None), GSSAPI automatically works with the currently active credentials cache,
            which should be associated with a simple user with limited permissions.
            When using `FreeIpaClient` with elevated privileges (e.g. to create users),
            we want to avoid using leaking elevated privileges outside a constrained context.
            See `acquire_gssapi_creds` to get non-default GSSAPI credentials based on a keytab file
            and an in-memory credentials cache.
        """
        # TODO: support for HA setup (multiple servers)?
        if not isinstance(ipa_server, str):
            raise ValueError(f"Invalid {ipa_server=}")
        if not ipa_server.startswith("http"):
            ipa_server = f"https://{ipa_server}"
        ipa_server = ipa_server.rstrip("/")
        _log.debug(f"Creating FreeIpaClient with {ipa_server=}, {verify_tls=}")
        self._server = ipa_server
        self._verify_tls = verify_tls
        self._api_version = api_version

        if requests_gssapi:
            _log.debug(f"Setting up FreeIpaClient with GSSAPI/Kerberos auth with {gssapi_creds.name=}")
            self._auth = requests_gssapi.HTTPSPNEGOAuth(
                opportunistic_auth=opportunistic_auth,
                creds=gssapi_creds,
            )
        else:
            _log.warning("Setting up FreeIpaClient without (GSSAPI/Kerberos) auth")
            self._auth = None

    def _do_request(
        self,
        ipa_method: str,
        *,
        arguments: Optional[Iterable[str]] = None,
        options: Optional[Mapping] = None,
        raise_on_error: bool = True,
    ) -> FreeIpaRpcResponse:
        """
        Do FreeIPA JSON-RPC request.

        :param ipa_method: FreeIPA API method name
        :param arguments: list of positional arguments
        :param options: dict of named options
        """
        request_id = generate_unique_id(prefix="ipa-r")
        post_data = {
            "method": ipa_method,
            "params": [
                list(arguments or []),
                {**(options or {}), "version": self._api_version},
            ],
            "id": request_id,
        }
        _log.debug("Sending JSON-RPC request: %s", post_data)
        response = requests.post(
            f"{self._server}/ipa/json",
            json=post_data,
            verify=self._verify_tls,
            headers={"Referer": f"{self._server}/ipa"},
            auth=self._auth,
        )
        response.raise_for_status()
        result = response.json()
        _log.debug("Got JSON-RPC response: %s", result)
        assert result["id"] == request_id

        response = FreeIpaRpcResponse(result=result.get("result"), error=result.get("error"))
        if raise_on_error:
            response.raise_on_error()
        return response

    def user_show(self, uid: str) -> dict:
        """
        Fetch user information (as a dict) from given user id.
        Raises exception (code 4001 "NotFound") when no user found.
        """
        ipa_resp = self._do_request("user_show", arguments=[uid])
        return ipa_resp.result["result"]

    def user_find(self, uid: str) -> Union[dict, None]:
        """
        Search user by user id and return user info (as dict) or None when no user found.
        """
        ipa_resp = self._do_request("user_find", options={"uid": uid, "sizelimit": 2})
        users = ipa_resp.result["result"]
        if len(users) == 1:
            return users[0]
        elif len(users) == 0:
            return None
        else:
            _log.error(f"Multiple users found for {uid=}: {users=}")
            raise FreeIpaException(f"Multiple users found for {uid=}")

    def user_add(self, uid: str, first_name: str, last_name: str, email: str) -> dict:
        # TODO: work in progress here
        # TODO: support group related options?
        ipa_resp = self._do_request(
            "user_add",
            arguments=[uid],
            options={
                "givenname": first_name or "",
                "sn": last_name or "",
                "mail": email,
            },
        )
        return ipa_resp.result["result"]

    def who_am_i(self) -> dict:
        ipa_resp = self._do_request("whoami")
        return ipa_resp.result


def get_freeipa_server_from_env(
    *, env_var: str = "OPENEO_FREEIPA_SERVER", conf: Union[str, Path] = "/etc/ipa/default.conf"
) -> Union[str, None]:
    """Get the FreeIPA server from an env var or system-wide IPA configuration file ('/etc/ipa/default.conf')."""
    if os.environ.get(env_var):
        return os.environ[env_var]
    else:
        conf = Path(conf)
        if conf.exists():
            try:
                parser = configparser.ConfigParser()
                parser.read(conf)
                return parser["global"]["server"]
            except Exception as e:
                _log.warning(f"Failed to get FreeIPA server from {conf}: {e}")
    return None


def acquire_gssapi_creds(
    principal: str, keytab_path: Union[str, Path], ccache: str = "MEMORY:"
) -> "gssapi.Credentials":
    """
    Helper to acquire initial GSSAPI credentials based on a keytab file.

    Additionally by using a "MEMORY:" credential cache,
    we avoid leaking (elevated) credentials into existing credential caches on disk.

    :param principal: Kerberos principal (e.g. "openeo@EXAMPLE.COM")
    :param keytab_path: path to keytab file
    :param ccache: credential cache name (default: "MEMORY" to not leak into existing credential caches)
    :return:
    """
    _log.debug(f"Acquiring GSSAPI credentials for {principal=} with {keytab_path=}")
    return gssapi.Credentials(
        usage="initiate",
        name=gssapi.Name(principal),
        store={
            "client_keytab": f"FILE:{str(keytab_path)}",
            "ccache": ccache,
        },
    )


def get_verify_tls_from_env(*, env_var: str = "OPENEO_FREEIPA_VERIFY_TLS") -> Union[bool, str]:
    """Get `verify_tls` option from env var."""
    value = os.environ.get(env_var)
    if not value:
        # Verify by default
        return True
    elif value.startswith("/"):
        # Assume it is a path to a CA certificate file
        return value
    else:
        return smart_bool(value)


def main():
    """
    Very simple CLI tool to use FreeIpaClient from command line.

    Usage: set env vars for configuration and call with:
    first argument is the IPA method name, followed by positional arguments:

        export OPENEO_FREEIPA_SERVER=ipa.example.com
        export OPENEO_FREEIPA_VERIFY_TLS=false
        python -m openeogeotrellis.integrations.freeipa user_find john
    """
    logging.basicConfig(level=logging.DEBUG)
    ipa_client = FreeIpaClient(ipa_server=get_freeipa_server_from_env(), verify_tls=get_verify_tls_from_env())
    method = sys.argv[1]
    args = sys.argv[2:]
    _log.info("Calling %s with %s", method, args)
    result = getattr(ipa_client, method)(*args)
    print(result)


if __name__ == "__main__":
    main()
