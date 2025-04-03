from typing import Optional

import attrs


@attrs.frozen(kw_only=True)
class CalrissianConfig:

    namespace: str = "calrissian-demo-project"

    # TODO: proper calrissian image? Official one doesn't work due to https://github.com/Duke-GCB/calrissian/issues/124#issuecomment-947008286
    # calrissian_image: Optional[str] = "ghcr.io/duke-gcb/calrissian/calrissian:0.17.1"
    calrissian_image: str = "registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/calrissian:latest"

    s3_bucket: str = "calrissian"
