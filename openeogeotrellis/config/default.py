"""
Default GpsBackendConfig
"""
from openeo_driver.users.oidc import OidcProvider

from openeogeotrellis.config import GpsBackendConfig

oidc_providers = [
    OidcProvider(
        id="egi",
        issuer="https://aai.egi.eu/auth/realms/egi/",
        title="EGI Check-in",
    ),
]


config = GpsBackendConfig(
    id="default",
    oidc_providers=oidc_providers,
)
