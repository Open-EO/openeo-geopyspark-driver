"""
This file ties regions to cloud providers.
"""

# Providers
CF = "cf"  # CloudFerro
OTC = "otc"  # Open Telekom cloud
EODATA = "eodata"  # eodata is considered a separate cloud provider stack


def get_s3_provider(region_name: str) -> str:
    if region_name in ["waw3-1", "waw3-2", "waw4-1"]:
        return CF
    elif region_name in ["eu-nl", "eu-de"]:
        return OTC
    elif region_name in ["eodata"]:
        return EODATA
    raise NotImplementedError(f"Unsupported region {region_name}")
