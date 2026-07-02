
from __future__ import annotations

import abc
import json
import logging
from typing import Any, Dict, Optional, Type

logger = logging.getLogger(__name__)

# Registry: maps builder key → builder class
_BUILDERS: Dict[str, Type[DatacubeBuilder]] = {}


def register_builder(key: str):
    """Class decorator to register a DatacubeBuilder under a key."""
    def decorator(cls):
        _BUILDERS[key] = cls
        return cls
    return decorator


def get_builder(feature_flags: Dict[str, Any]) -> DatacubeBuilder:
    """
    Select the appropriate DatacubeBuilder based on feature_flags.

    - ``feature_flags["dggs"] = {"format": "healpix", "variant": "scalar", "nside": 128}``
      → selects the ``"healpix"`` builder.
    - No ``dggs`` key → selects the ``"default"`` builder (existing behavior).
    """
    dggs_config = feature_flags.get("dggs")
    if dggs_config:
        fmt = dggs_config.get("format", "healpix")
        builder_cls = _BUILDERS.get(fmt)
        if builder_cls is None:
            raise ValueError(
                f"No datacube builder registered for DGGS format {fmt!r}. "
                f"Available: {list(_BUILDERS.keys())}"
            )
        return builder_cls(dggs_config=dggs_config)

    return _BUILDERS["default"]()


class DatacubeBuilder(abc.ABC):
    """Strategy for building a datacube from a _LoadStacContext."""

    def __init__(self, dggs_config: Optional[Dict[str, Any]] = None):
        self.dggs_config = dggs_config or {}

    @abc.abstractmethod
    def build(self, context) -> Any:
        """Build and return a datacube (GeopysparkDataCube or HealpixDataCube)."""
        ...

@register_builder("default")
class DefaultDatacubeBuilder(DatacubeBuilder):
    """Existing PyramidFactory / NetCDFCollection loading logic."""

    def build(self, context):
        from openeogeotrellis.load_stac import _build_datacube
        return _build_datacube(context)


@register_builder("healpix")
class HealpixDatacubeBuilder(DatacubeBuilder):
    """
    Build a HealpixDataCube via Sentinel3BinningReader or synthetic data.

    Expected dggs_config keys:
        format: "healpix"
        variant: "scalar" | "packed"  (default: "scalar")
        nside: int                    (default: 128)
        source: "sentinel3" | "synthetic"  (default: "sentinel3")

        # Sentinel-3 specific:
        band_variables: list[str]
        lat_variable: str             (default: "latitude")
        lon_variable: str             (default: "longitude")
        geo_file_suffix: str | None
        aggregation: "mean" | "first" (default: "mean")

        # Synthetic specific:
        synthetic_generator: "random" | "fractal" | "latitude_stripes"
    """

    def build(self, context):
        from openeogeotrellis.healpixdatacube import HealpixDataCube
        from openeogeotrellis.utils import get_jvm

        jvm = context.jvm
        nside = self.dggs_config.get("nside", 128)
        source = self.dggs_config.get("source", "sentinel3")
        variant = self.dggs_config.get("variant", "scalar")

        if source == "sentinel3":
            cube = self._load_sentinel3(context, jvm, nside)
        elif source == "synthetic":
            cube = self._load_synthetic(context, jvm, nside, variant)
        else:
            raise ValueError(f"Unknown healpix source: {source!r}")

        return HealpixDataCube(cube, metadata=context.metadata)

    def _load_sentinel3(self, context, jvm, nside):
        cfg = self.dggs_config
        band_vars = cfg.get("band_variables", [])


        config = jvm.org.openeo.geotrellishealpix.ProductConfigJsonLoader.fromJson(json.dumps(cfg))


        reader = jvm.org.openeo.geotrellishealpix.Sentinel3BinningReader
        return reader.datacube_seq(
            context.opensearch_client,
            context.url,  # openSearchCollectionId
            context.projected_polygons,
            context.from_date.isoformat(),
            context.to_date.isoformat(),
            context.metadata_properties,
            context.correlation_id,
            context.data_cube_parameters,
            nside,
            config
        )

    def _load_synthetic(self, context, jvm, nside, variant):
        import datetime as dt

        generator = jvm.org.openeo.geotrellishealpix.HealpixDataGenerator
        gen_type = self.dggs_config.get("synthetic_generator", "random")
        bands = self.dggs_config.get("band_variables", ["band0"])

        timestamps = jvm.scala.collection.immutable.List.empty()
        # Build timestamp list from context
        ts = jvm.java.sql.Timestamp.valueOf(
            context.from_date.strftime("%Y-%m-%d %H:%M:%S")
        )
        #timestamps = timestamps.$colon$colon(ts)

        if gen_type == "random":
            return generator.randomScalar(
                jvm.org.apache.spark.sql.SparkSession.active(),
                nside, timestamps, bands, 42
            )
        elif gen_type == "latitude_stripes":
            return generator.latitudeStripesScalar(
                jvm.org.apache.spark.sql.SparkSession.active(),
                nside, timestamps, bands[0] if bands else "lat"
            )
        else:
            raise ValueError(f"Unknown synthetic generator: {gen_type!r}")