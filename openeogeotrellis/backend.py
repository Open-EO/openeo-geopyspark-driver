from typing import List

import geopyspark as gps
from geopyspark import TiledRasterLayer, LayerType
from openeo_driver import backend
from py4j.java_gateway import JavaGateway

from openeo_driver.backend import ServiceMetadata
from openeogeotrellis import ConfigParams
from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.service_registry import InMemoryServiceRegistry, ZooKeeperServiceRegistry, AbstractServiceRegistry
from openeogeotrellis.utils import kerberos, normalize_date


class GpsSecondaryServices(backend.SecondaryServices):
    """Secondary Services implementation for GeoPySpark backend"""

    def __init__(self, service_registry: AbstractServiceRegistry):
        self.service_registry = service_registry

    def service_types(self) -> dict:
        return {
            "WMTS": {
                "configuration": {
                    "version": {
                        "type": "string",
                        "description": "The WMTS version to use.",
                        "default": "1.0.0",
                        "enum": [
                            "1.0.0"
                        ]
                    }
                },
                # TODO?
                "process_parameters": [],
                "links": [],
            }
        }

    def list_services(self) -> List[ServiceMetadata]:
        return list(self.service_registry.get_metadata_all().values())

    def service_info(self, service_id: str) -> ServiceMetadata:
        return self.service_registry.get_metadata(service_id)

    def remove_service(self, service_id: str) -> None:
        self.service_registry.stop_service(service_id)


class GeoPySparkBackendImplementation(backend.OpenEoBackendImplementation):

    def __init__(self):
        # TODO: do this with a config instead of hardcoding rules?
        self._service_registry = (
            InMemoryServiceRegistry() if ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )

        super().__init__(
            secondary_services=GpsSecondaryServices(service_registry=self._service_registry),
            catalog=get_layer_catalog(service_registry=self._service_registry),
        )

    def health_check(self) -> str:
        from pyspark import SparkContext
        sc = SparkContext.getOrCreate()
        count = sc.parallelize([1, 2, 3]).count()
        return 'Health check: ' + str(count)

    def file_formats(self) -> dict:
        return {
            "input": {
                "GeoJSON": {
                    "gis_data_type": ["vector"]
                }
            },
            "output": {
                "GTiff": {
                    "title": "GeoTiff",
                    "gis_data_types": ["raster"],
                },
                "CovJSON": {
                    "gis_data_types": ["other"],  # TODO: also "raster", "vector", "table"?
                },
                "NetCDF": {
                    "gis_data_types": ["other"],  # TODO: also "raster", "vector", "table"?
                },
            },
        }

    def load_disk_data(self, format: str, glob_pattern: str, options: dict, viewing_parameters: dict) -> object:
        if format != 'GTiff':
            raise NotImplementedError("The format is not supported by the backend: " + format)

        date_regex = options['date_regex']

        if glob_pattern.startswith("hdfs:"):
            kerberos()

        from_date = normalize_date(viewing_parameters.get("from", None))
        to_date = normalize_date(viewing_parameters.get("to", None))

        left = viewing_parameters.get("left", None)
        right = viewing_parameters.get("right", None)
        top = viewing_parameters.get("top", None)
        bottom = viewing_parameters.get("bottom", None)
        srs = viewing_parameters.get("srs", None)
        band_indices = viewing_parameters.get("bands")

        sc = gps.get_spark_context()

        gateway = JavaGateway(eager_load=True, gateway_parameters=sc._gateway.gateway_parameters)
        jvm = gateway.jvm

        extent = jvm.geotrellis.vector.Extent(float(left), float(bottom), float(right), float(top)) \
            if left is not None and right is not None and top is not None and bottom is not None else None

        pyramid = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex) \
            .pyramid_seq(extent, srs, from_date, to_date)

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option
        levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
            option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                  range(0, pyramid.size())}

        image_collection = GeotrellisTimeSeriesImageCollection(
            pyramid=gps.Pyramid(levels),
            service_registry=self._service_registry,
            metadata={}
        )

        return image_collection.band_filter(band_indices) if band_indices else image_collection


def get_openeo_backend_implementation() -> backend.OpenEoBackendImplementation:
    return GeoPySparkBackendImplementation()
