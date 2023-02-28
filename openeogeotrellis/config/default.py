"""
Default GpsBackendConfig
"""
from openeo_driver.users.oidc import OidcProvider

from openeogeotrellis.config import GpsBackendConfig

oidc_default_client_egi = {
    "id": "vito-default-client",
    "grant_types": [
        "authorization_code+pkce",
        "urn:ietf:params:oauth:grant-type:device_code+pkce",
        "refresh_token",
    ],
    "redirect_urls": [
        "https://editor.openeo.org",
        "http://localhost:1410/",
    ],
}

oidc_providers = [
    OidcProvider(
        id="egi",
        issuer="https://aai.egi.eu/auth/realms/egi/",
        scopes=[
            "openid",
            "email",
            "eduperson_entitlement",
            "eduperson_scoped_affiliation",
        ],
        title="EGI Check-in",
        default_clients=[oidc_default_client_egi],
    ),
    # TODO: provide only one EGI Check-in variation? Or only include EGI Check-in dev instance on openeo-dev?
    OidcProvider(
        id="egi-dev",
        issuer="https://aai-dev.egi.eu/auth/realms/egi",
        scopes=[
            "openid",
            "email",
            "eduperson_entitlement",
            "eduperson_scoped_affiliation",
        ],
        title="EGI Check-in (dev)",
        default_clients=[oidc_default_client_egi],
    ),
    OidcProvider(
        id="keycloak",
        issuer="https://sso.vgt.vito.be/auth/realms/terrascope",
        scopes=["openid", "email"],
        title="VITO Keycloak",
    ),
]

config = GpsBackendConfig(
    oidc_providers=oidc_providers,
)
