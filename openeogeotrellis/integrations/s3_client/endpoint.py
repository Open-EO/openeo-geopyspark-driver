import os
from typing import Callable

from openeogeotrellis.integrations.s3_client.providers import CF, OTC, get_s3_provider, EODATA, UNKNOWN


def get_cf_endpoint(region_name: str) -> str:
    return f"https://s3.{region_name}.cloudferro.com"


def get_otc_endpoint(region_name: str) -> str:
    return f"https://obs.{region_name}.otc.t-systems.com"


def get_eodata_endpoint(_: str) -> str:
    """
    EODATA is a special case because it is often accessed via a private path which differs per environment.
    As such the details need to be extracted from the execution environment. We recommend a specific environment
    variable but fallback to legacy values
    """
    try:
        return os.environ["EODATA_S3_ENDPOINT"]
    except KeyError:
        try:
            endpoint_without_protocol = os.environ["AWS_S3_ENDPOINT"]
            if os.environ["AWS_HTTPS"] == "NO":
                return f"http://{endpoint_without_protocol}"
            else:
                return f"https://{endpoint_without_protocol}"
        except KeyError as ke:
            raise EnvironmentError("No valid config for eodata access.") from ke


def get_legacy_default_endpoint(region_name: str) -> str:
    """
    This is a fallback function for when the geopyspark-driver is not aware of the cloud region nor provider.
    It falls back to legacy behavior of using a pre-defined SWIFT_URL as long as that one is not empty since
    that would mean a misconfigurations and leave to more obscure errors during execution.
    """
    legacy_fallback = os.environ.get("SWIFT_URL")
    if legacy_fallback is None:
        raise EnvironmentError(f"Unsupported region {region_name} and no fallback via SWIFT_URL")
    return legacy_fallback


def get_endpoint_builder(provider_name: str) -> Callable[[str], str]:
    if provider_name == CF:
        return get_cf_endpoint
    elif provider_name == OTC:
        return get_otc_endpoint
    elif provider_name == EODATA:
        return get_eodata_endpoint
    elif provider_name == UNKNOWN:
        return get_legacy_default_endpoint
    raise NotImplementedError(f"Unsupported provider {provider_name}")


def get_endpoint(region_name) -> str:
    provider = get_s3_provider(region_name)
    return get_endpoint_builder(provider)(region_name)
