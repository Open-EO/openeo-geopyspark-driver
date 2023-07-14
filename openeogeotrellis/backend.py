import random

import datetime
import json
import logging
import os
import stat
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
from decimal import Decimal
from functools import lru_cache, partial, reduce

from pandas import Timedelta
from pathlib import Path
from subprocess import CalledProcessError
from typing import Callable, Dict, Tuple, Optional, List, Union, Iterable, Mapping
from urllib.parse import urlparse

import flask
import geopyspark as gps
import pkg_resources
import requests
import shapely.geometry.base
from deprecated import deprecated
from geopyspark import TiledRasterLayer, LayerType
from py4j.java_gateway import JVMView, JavaObject
from py4j.protocol import Py4JJavaError
from pyspark import SparkContext
from pyspark.mllib.tree import RandomForestModel
from pyspark.version import __version__ as pysparkversion
import pystac
from shapely.geometry import box, Polygon

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.metadata import TemporalDimension, SpatialDimension, Band, BandDimension
import openeo.udf
from openeo.util import (
    dict_no_none,
    rfc3339,
    deep_get,
    repr_truncate,
    str_truncate,
    TimingLogger,
)
from openeo_driver import backend
from openeo_driver.ProcessGraphDeserializer import ConcreteProcessing, ENV_SAVE_RESULT
from openeo_driver.backend import (ServiceMetadata, BatchJobMetadata, OidcProvider, ErrorSummary, LoadParameters,
                                   CollectionCatalog)
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import (JobNotFinishedException, OpenEOApiException, InternalException,
                                  ServiceUnsupportedException, FeatureUnsupportedException, NoDataAvailableException)
from openeo_driver.jobregistry import ElasticJobRegistry, JOB_STATUS, DEPENDENCY_STATUS
from openeo_driver.save_result import ImageCollectionResult
from openeo_driver.users import User
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.util.logging import (
    FlaskRequestCorrelationIdLogging,
    FlaskUserIdLogging,
)
from openeo_driver.util.http import requests_with_retry
from openeo_driver.util.utm import area_in_square_meters, auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv, to_hashable, generate_unique_id
from openeogeotrellis import sentinel_hub
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import (
    GeopysparkDataCube,
    GeopysparkCubeMetadata,
)
from openeogeotrellis.geotrellis_tile_processgraph_visitor import (
    GeotrellisTileProcessGraphVisitor,
)
from openeogeotrellis.integrations.etl_api import EtlApi, get_etl_api_access_token
from openeogeotrellis.integrations.kubernetes import (
    truncate_job_id_k8s,
    k8s_job_name,
    kube_client,
)
from openeogeotrellis.job_registry import (
    ZkJobRegistry,
    zk_job_info_to_metadata,
    DoubleJobRegistry,
)
from openeogeotrellis.layercatalog import (get_layer_catalog, check_missing_products, GeoPySparkLayerCatalog,
                                           is_layer_too_large)
from openeogeotrellis.logs import elasticsearch_logs
from openeogeotrellis.ml.GeopySparkCatBoostModel import CatBoostClassificationModel
from openeogeotrellis.sentinel_hub.batchprocessing import (
    SentinelHubBatchProcessing,
)
from openeogeotrellis.service_registry import (
    InMemoryServiceRegistry,
    ZooKeeperServiceRegistry,
    AbstractServiceRegistry,
    SecondaryService,
    ServiceEntity,
)
from openeogeotrellis.traefik import Traefik
from openeogeotrellis.udf import run_udf_code
from openeogeotrellis.user_defined_process_repository import (
    ZooKeeperUserDefinedProcessRepository,
    InMemoryUserDefinedProcessRepository,
)
from openeogeotrellis.utils import (
    kerberos,
    zk_client,
    to_projected_polygons,
    normalize_temporal_extent,
    dict_merge_recursive,
    single_value,
    add_permissions,
    get_jvm,
    mdc_include,
    mdc_remove,
    get_s3_file_contents,
    s3_client,
)
from openeogeotrellis.vault import Vault

JOB_METADATA_FILENAME = "job_metadata.json"

logger = logging.getLogger(__name__)


class GpsSecondaryServices(backend.SecondaryServices):
    """Secondary Services implementation for GeoPySpark backend"""

    def __init__(self, service_registry: AbstractServiceRegistry):
        self.service_registry = service_registry

    def service_types(self) -> dict:
        return {
            "WMTS": {
                "title": "Web Map Tile Service",
                "configuration": {
                    "version": {
                        "type": "string",
                        "description": "The WMTS version to use.",
                        "default": "1.0.0",
                        "enum": [
                            "1.0.0"
                        ]
                    },
                    "colormap": {
                        "type": "string",
                        "description": "The colormap to apply to single band layers",
                        "default": "YlGn"
                    }
                },
                "process_parameters": [
                    # TODO: we should at least have bbox and time range parameters here
                ],
                "links": [],
            }
        }

    def list_services(self, user_id: str) -> List[ServiceMetadata]:
        return list(self.service_registry.get_metadata_all(user_id).values())

    def service_info(self, user_id: str, service_id: str) -> ServiceMetadata:
        return self.service_registry.get_metadata(user_id=user_id, service_id=service_id)

    def remove_service(self, user_id: str, service_id: str) -> None:
        self.service_registry.stop_service(user_id=user_id, service_id=service_id)
        self._unproxy_service(service_id)

    def remove_services_before(self, upper: datetime.datetime) -> None:
        user_services = self.service_registry.get_metadata_all_before(upper)

        for user_id, service in user_services:
            self.service_registry.stop_service(user_id=user_id, service_id=service.id)
            self._unproxy_service(service.id)

    def _create_service(self, user_id: str, process_graph: dict, service_type: str, api_version: str,
                       configuration: dict) -> str:
        # TODO: reduce code duplication between this and start_service()
        from openeo_driver.ProcessGraphDeserializer import evaluate

        if service_type.lower() != 'wmts':
            raise ServiceUnsupportedException(service_type)

        service_id = generate_unique_id(prefix="s")

        image_collection = evaluate(
            process_graph,
            env=EvalEnv({
                'version': api_version,
                'pyramid_levels': 'all',
                "backend_implementation": GeoPySparkBackendImplementation(),
            })
        )

        if (isinstance(image_collection, ImageCollectionResult)):
            image_collection = image_collection.cube
        elif(not isinstance(image_collection,GeopysparkDataCube)):
            logger.info("Can not create service for: " + str(image_collection))
            raise OpenEOApiException("Can not create service for: " + str(image_collection))


        wmts_base_url = os.getenv('WMTS_BASE_URL_PATTERN', 'http://openeo.vgt.vito.be/openeo/services/%s') % service_id

        self.service_registry.persist(user_id, ServiceMetadata(
            id=service_id,
            process={"process_graph": process_graph},
            url=wmts_base_url + "/service/wmts",
            type=service_type,
            enabled=True,
            attributes={},
            configuration=configuration,
            created=datetime.datetime.utcnow()), api_version)

        secondary_service = self._wmts_service(image_collection, configuration, wmts_base_url)

        self.service_registry.register(service_id, secondary_service)
        self._proxy_service(service_id, secondary_service.host, secondary_service.port)

        return service_id

    def start_service(self, user_id: str, service_id: str) -> None:
        from openeo_driver.ProcessGraphDeserializer import evaluate

        service: ServiceEntity = self.service_registry.get(user_id=user_id, service_id=service_id)
        service_metadata: ServiceMetadata = service.metadata

        service_type = service_metadata.type
        process_graph = service_metadata.process["process_graph"]
        api_version = service.api_version
        configuration = service_metadata.configuration

        if service_type.lower() != 'wmts':
            raise ServiceUnsupportedException(service_type)

        image_collection: GeopysparkDataCube = evaluate(
            process_graph,
            env=EvalEnv({
                'version': api_version,
                'pyramid_levels': 'all',
                "backend_implementation": GeoPySparkBackendImplementation(),
            })
        )

        wmts_base_url = os.getenv('WMTS_BASE_URL_PATTERN', 'http://openeo.vgt.vito.be/openeo/services/%s') % service_id

        secondary_service = self._wmts_service(image_collection, configuration, wmts_base_url)

        self.service_registry.register(service_id, secondary_service)
        self._proxy_service(service_id, secondary_service.host, secondary_service.port)

    def _wmts_service(self, image_collection, configuration: dict, wmts_base_url: str) -> SecondaryService:
        random_port = 0

        jvm = get_jvm()
        wmts = jvm.be.vito.eodata.gwcgeotrellis.wmts.WMTSServer.createServer(random_port, wmts_base_url)
        logger.info('Created WMTSServer: {w!s} ({u!s}/service/wmts, {p!r})'.format(w=wmts, u=wmts.getURI(), p=wmts.getPort()))

        if "colormap" in configuration:
            max_zoom = image_collection.pyramid.max_zoom
            min_zoom = min(image_collection.pyramid.levels.keys())
            reduced_resolution = max(min_zoom,max_zoom-4)
            if reduced_resolution not in image_collection.pyramid.levels:
                reduced_resolution = min_zoom
            histogram = image_collection.pyramid.levels[reduced_resolution].get_histogram()
            matplotlib_name = configuration.get("colormap", "YlGn")

            #color_map = gps.ColorMap.from_colors(breaks=[x for x in range(0,250)], color_list=gps.get_colors_from_matplotlib("YlGn"))
            color_map = gps.ColorMap.build(histogram, matplotlib_name)
            srdd_dict = {k: v.srdd.rdd() for k, v in image_collection.pyramid.levels.items()}
            wmts.addPyramidLayer("RDD", srdd_dict,color_map.cmap)
        else:
            srdd_dict = {k: v.srdd.rdd() for k, v in image_collection.pyramid.levels.items()}
            wmts.addPyramidLayer("RDD", srdd_dict)

        import socket
        # TODO what is this host logic about?
        host = [l for l in
                          ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                           [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                             [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]])
                          if l][0][0]

        return SecondaryService(host=host, port=wmts.getPort(), server=wmts)

    def restore_services(self):
        for user_id, service_metadata in self.service_registry.get_metadata_all_before(upper=datetime.datetime.max):
            if service_metadata.enabled:
                try:
                    self.start_service(user_id=user_id, service_id=service_metadata.id)
                except:
                    logger.exception("Error while restoring service: " + str(service_metadata))

    def _proxy_service(self, service_id, host, port):
        if not ConfigParams().is_ci_context:
            with zk_client() as zk:
                Traefik(zk).proxy_service(service_id, host, port)

    def _unproxy_service(self, service_id):
        if not ConfigParams().is_ci_context:
            with zk_client() as zk:
                Traefik(zk).unproxy_service(service_id)


class SingleNodeUDFProcessGraphVisitor(ProcessGraphVisitor):

    def __init__(self):
        super().__init__()
        self.udf_args = {}


    def enterArgument(self, argument_id: str, value):
        self.udf_args[argument_id] = value

    def constantArgument(self, argument_id: str, value):
        self.udf_args[argument_id] = value


class GeoPySparkBackendImplementation(backend.OpenEoBackendImplementation):
    def __init__(
            self,
            use_zookeeper=True,
            batch_job_output_root: Optional[Path] = None,
            use_job_registry: bool = True,
            elastic_job_registry: Optional[ElasticJobRegistry] = None,
            use_etl_api: bool = False,
    ):
        self._service_registry = (
            # TODO #283 #285: eliminate is_ci_context, use some kind of config structure
            InMemoryServiceRegistry() if not use_zookeeper or ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )

        user_defined_processes = (
            # TODO #283 #285: eliminate is_ci_context, use some kind of config structure
            InMemoryUserDefinedProcessRepository() if not use_zookeeper or ConfigParams().is_ci_context
            else ZooKeeperUserDefinedProcessRepository(hosts=ConfigParams().zookeepernodes)
        )

        requests_session = requests_with_retry(total=3, backoff_factor=2)
        vault = Vault(ConfigParams().vault_addr, requests_session)

        catalog = get_layer_catalog(vault)

        jvm = get_jvm()

        conf = SparkContext.getOrCreate().getConf()
        principal = conf.get("spark.yarn.principal", conf.get("spark.kerberos.principal"))
        key_tab = conf.get("spark.yarn.keytab", conf.get("spark.kerberos.keytab"))

        if use_job_registry and not elastic_job_registry:
            # TODO #236 avoid this fallback and just make sure it is always set when necessary
            logger.warning("No elastic_job_registry given to GeoPySparkBackendImplementation, creating one")
            elastic_job_registry = get_elastic_job_registry()

        # Start persistent workers if configured.
        config_params = ConfigParams()
        persistent_worker_count = config_params.persistent_worker_count
        if persistent_worker_count != 0 and not config_params.is_kube_deploy:
            shutdown_file = config_params.persistent_worker_dir / "shutdown"
            if shutdown_file.exists():
                shutdown_file.unlink()
            persistent_script_path = pkg_resources.resource_filename('openeogeotrellis.deploy', "submit_persistent_worker.sh")
            for i in range(persistent_worker_count):
                args = [
                    persistent_script_path, str(i),
                    principal, key_tab,
                    GpsBatchJobs.get_submit_py_files(),
                    "INFO"
                ]
                logger.info(f"Submitting persistent worker {i} with args: {args!r}")
                output_string = subprocess.check_output(args, stderr = subprocess.STDOUT, universal_newlines = True)
                logger.info(f"Submitted persistent worker {i}, output was: {output_string}")


        super().__init__(
            catalog=catalog,
            batch_jobs=GpsBatchJobs(
                catalog=catalog,
                jvm=jvm,
                principal=principal,
                key_tab=key_tab,
                vault=vault,
                output_root_dir=batch_job_output_root,
                elastic_job_registry=elastic_job_registry,
            ),
            user_defined_processes=user_defined_processes,
            processing=GpsProcessing(),
            # secondary_services=GpsSecondaryServices(service_registry=self._service_registry),
        )

        self._principal = principal
        self._key_tab = key_tab

        self._get_request_costs: Callable[[str, str, bool], Optional[float]] = lambda user_id, request_id, success: None

        if use_etl_api:
            def get_request_costs_from_etl_api(user_id: str, request_id: str, success: bool) -> Optional[float]:
                sc = SparkContext.getOrCreate()

                # TODO: replace get-or-create with a plain get to avoid unnecessary Spark accumulator creation?
                request_metadata_tracker = jvm.org.openeo.geotrelliscommon.ScopedMetadataTracker.apply(request_id,
                                                                                                       sc._jsc.sc())
                sentinel_hub_processing_units = request_metadata_tracker.sentinelHubProcessingUnits()

                if sentinel_hub_processing_units > 0:
                    if config_params.is_kube_deploy:
                        # TODO: replace with strategy pattern?
                        etl_api_client_id = os.environ["OPENEO_ETL_OIDC_CLIENT_ID"]
                        etl_api_client_secret = os.environ["OPENEO_ETL_OIDC_CLIENT_SECRET"]
                    else:
                        vault_token = vault.login_kerberos(self._principal, self._key_tab)
                        etl_api_credentials = vault.get_etl_api_credentials(vault_token)
                        etl_api_client_id = etl_api_credentials.client_id
                        etl_api_client_secret = etl_api_credentials.client_secret

                    access_token = get_etl_api_access_token(client_id=etl_api_client_id,
                                                            client_secret=etl_api_client_secret,
                                                            requests_session=requests_session)

                    etl_api = EtlApi(ConfigParams().etl_api, requests_session)

                    costs = etl_api.log_resource_usage(batch_job_id=request_id,
                                                       title=None,
                                                       execution_id=request_id,
                                                       user_id=user_id,
                                                       started_ms=None,
                                                       finished_ms=None,
                                                       state="FINISHED" if success else "FAILED",
                                                       status="SUCCEEDED" if success else "FAILED",
                                                       cpu_seconds=None,
                                                       mb_seconds=None,
                                                       duration_ms=None,
                                                       sentinel_hub_processing_units=sentinel_hub_processing_units,
                                                       access_token=access_token)

                    logger.info(f"{'successful' if success else 'failed'} request required "
                                f"{sentinel_hub_processing_units} PU(s) and cost {costs} credit(s)")

                    return costs

                return None

            self._get_request_costs = get_request_costs_from_etl_api

    def capabilities_billing(self) -> dict:
        return {
            "currency": "credits",
        }

    def health_check(self, options: Optional[dict] = None) -> dict:
        mode = (options or {}).get("mode", "spark")
        if mode == "spark":
            # Check if we have a working (Py)Spark context
            sc = SparkContext.getOrCreate()
            count = sc.parallelize([1, 2, 3], numSlices=2).map(lambda x: x * x).sum()
            res = {"mode": "spark", "status": "OK" if count == 14 else "FAIL", "count": count}
        elif mode == "jvm":
            # Check if we have a working jvm context
            jvm = get_jvm()
            pi = jvm.Math.PI
            res = {"mode": "jvm", "status": "OK" if repr(pi).startswith("3.14") else "FAIL", "pi": repr(jvm.Math.PI)}
        else:
            res = {"mode": "basic", "status": "OK"}
        return res

    def oidc_providers(self) -> List[OidcProvider]:
        return get_backend_config().oidc_providers

    def file_formats(self) -> dict:
        return {
            "input": {
                "GeoJSON": {
                    "description": "GeoJSON allows sending vector data as part of your JSON request. GeoJSON is always in EPSG:4326. ",
                    "gis_data_types": ["vector"],
                    "parameters": {},
                },
                "Parquet": {
                    "description": "GeoParquet is an efficient binary format, to distribute large amounts of vector data.",
                    "gis_data_types": ["vector"],
                    "parameters": {},
                },
                "GTiff": {
                    "description": "Geotiff is one of the most widely supported formats. This backend allows reading from Geotiff to create raster data cubes.",
                    "gis_data_types": ["raster"],
                    "parameters": {}
                }
            },
            "output": {
                "GTiff": {
                    "title": "GeoTiff",
                    "description": "Cloud Optimized Geotiff is one of the most widely supported formats and thus a popular choice for further dissemination. This implementation stores all bands in one file, and creates one file per timestamp in your datacube.",
                    "gis_data_types": ["raster"],
                    "parameters": {
                        "tile_grid": {
                            "type": ["string", "null"],
                            "description": "Specifies the tile grid to use, for batch jobs only. By default, no tile grid is set, and one Geotiff is generated per date. If a tile grid is set, multiple geotiffs are generated per date, as defined by the specified tiling grid.",
                            "default": None,
                            "enum": ["wgs84-1degree", "utm-100km", "utm-20km", "utm-10km"]
                        },
                        "ZLEVEL": {
                            "type": "string",
                            "description": "Specifies the compression level used for DEFLATE compression. As a number from 1 to 9, lowest and fastest compression is 1 while 9 is highest and slowest compression.",
                            "default": "6"
                        },
                        "sample_by_feature": {
                            "type": "boolean",
                            "default": False,
                            "description": "Set to true to write one output tiff per feature and date. Spatial features can be specified using filter_spatial. This setting is used to sample a data cube at multiple locations in a single job."
                        },
                        "feature_id_property": {
                            "type": ["string", "null"],
                            "default": None,
                            "description": "Specifies the name of the feature attribute that is to be used as feature id, by processes that require it. Can be used to link a given output back to an input feature."
                        },
                        "overviews": {
                            "type": "string",
                            "description": "Specifies the strategy to generate overviews. The default, AUTO, allows the backend to choose an optimal configuration, depending on the size of the generated tiff, and backend capabilities.",
                            "default": "AUTO",
                            "enum": ["AUTO", "OFF"]
                        },
                        "colormap": {
                            "type": ["object", "null"],
                            "description": "Allows specifying a colormap, for single band geotiffs. The colormap is a dictionary mapping band values to colors, specified by an integer.",
                            "default": None
                        },
                        "filename_prefix": {
                            "type": "string",
                            "description": "Specifies the filename prefix when outputting multiple files. By default, depending on the context, 'OpenEO' or a part of the input filename will be used as prefix.",
                            "default": None,
                        },
                    },
                },
                "PNG": {
                    "title": "Portable Network Graphics",
                    "gis_data_types": ["raster"],
                    "parameters": {
                        "colormap": {
                            "type": ["object", "null"],
                            "description": "Allows specifying a colormap, for single band PNGs. The colormap is a dictionary mapping band values to colors, either specified by an integer or an array of [R, G, B, A], where each value lies between 0.0 and 1.0.",
                            "default": None
                        },
                    }
                },
                "CovJSON": {
                    "title": "CoverageJSON",
                    "gis_data_types": ["other","raster"],  # TODO: also "raster", "vector", "table"?
                    "parameters": {},
                },
                "netCDF": {
                    "title": "Network Common Data Form",
                    "description":"netCDF files allow to accurately represent an openEO datacube and its metadata.",
                    "gis_data_types": ["other", "raster"],  # TODO: also "raster", "vector", "table"?
                    "parameters": {
                        "sample_by_feature": {
                            "type": "boolean",
                            "default": False,
                            "description": "Set to true to write one output netCDF per feature, containing all bands and dates. Spatial features can be specified using filter_spatial. This setting is used to sample a data cube at multiple locations in a single job."
                        },
                        "feature_id_property": {
                            "type": ["string", "null"],
                            "default": None,
                            "description": "Specifies the name of the feature attribute that is to be used as feature id, by processes that require it. Can be used to link a given output back to an input feature."
                        }
                    },
                },
                "JSON": {
                    "gis_data_types": ["raster", "vector"],
                    "parameters": {},
                },
                "CSV": {
                    "title": "Comma Separated Values",
                    "description": "CSV format is supported to export vector cube data, for instance generated by aggregate_spatial.",
                    "gis_data_types": [ "vector"],
                    "parameters": {}
                }
            }
        }

    def load_disk_data(
            self, format: str, glob_pattern: str, options: dict, load_params: LoadParameters, env: EvalEnv
    ) -> GeopysparkDataCube:
        logger.info("load_disk_data with format {f!r}, glob {g!r}, options {o!r} and load params {p!r}".format(
            f=format, g=glob_pattern, o=options, p=load_params
        ))
        if format != 'GTiff':
            raise NotImplementedError("The format is not supported by the backend: " + format)

        date_regex = options['date_regex']

        if glob_pattern.startswith("hdfs:"):
            kerberos(self._principal, self._key_tab)

        metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
            # TODO: detect actual dimensions instead of this simple default?
            SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
            TemporalDimension(name='t', extent=[]), BandDimension(name="bands", bands=[Band("unknown")])
        ])

        # TODO: eliminate duplication with GeoPySparkLayerCatalog.load_collection
        temporal_extent = load_params.temporal_extent
        from_date, to_date = normalize_temporal_extent(temporal_extent)
        metadata = metadata.filter_temporal(from_date, to_date)

        spatial_extent = load_params.spatial_extent
        if len(spatial_extent) == 0:
            spatial_extent = load_params.global_extent

        west = spatial_extent.get("west", None)
        east = spatial_extent.get("east", None)
        north = spatial_extent.get("north", None)
        south = spatial_extent.get("south", None)
        crs = spatial_extent.get("crs", None)
        spatial_bounds_present = all(b is not None for b in [west, south, east, north])
        if spatial_bounds_present:
            metadata = metadata.filter_bbox(west=west, south=south, east=east, north=north, crs=crs)

        bands = load_params.bands
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
        else:
            band_indices = None

        jvm = get_jvm()

        feature_flags = load_params.get("featureflags", {})
        experimental = feature_flags.get("experimental", False)
        datacubeParams, single_level = self.catalog.create_datacube_parameters(load_params, env)

        extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north)) \
            if spatial_bounds_present else None

        factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex)
        if single_level:
            if extent is None:
                raise ValueError(f"Trying to load disk collection {glob_pattern} without extent.")
            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, crs or "EPSG:4326")
            pyramid = factory.datacube_seq(projected_polygons, from_date, to_date, {},"", datacubeParams)
        else:
            pyramid = (factory.pyramid_seq(extent, crs, from_date, to_date))

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option
        levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
            option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                  range(0, pyramid.size())}

        image_collection = GeopysparkDataCube(
            pyramid=gps.Pyramid(levels),
            metadata=metadata
        )

        return image_collection.filter_bands(band_indices) if band_indices else image_collection

    def load_result(self, job_id: str, user_id: Optional[str], load_params: LoadParameters,
                    env: EvalEnv) -> GeopysparkDataCube:
        logger.info("load_result from job ID {j!r} with load params {p!r}".format(j=job_id, p=load_params))

        requested_bbox = BoundingBox.from_dict_or_none(
            load_params.spatial_extent, default_crs="EPSG:4326"
        )
        logger.info(f"{requested_bbox=}")

        if job_id.startswith("http://") or job_id.startswith("https://"):
            job_results_canonical_url = job_id
            job_results = pystac.Collection.from_file(href=job_results_canonical_url)

            def intersects_spatial_extent(item) -> bool:
                if not requested_bbox or item.bbox is None:
                    return True

                requested_bbox_lonlat = requested_bbox.reproject("EPSG:4326")
                return requested_bbox_lonlat.as_polygon().intersects(
                    Polygon.from_bounds(*item.bbox)
                )

            uris_with_metadata = {asset.get_absolute_href(): (item.datetime.isoformat(),
                                                              asset.extra_fields.get("eo:bands", []))
                                  for item in job_results.get_items()
                                  if intersects_spatial_extent(item)
                                  for asset in item.get_assets().values()
                                  if asset.media_type == "image/tiff; application=geotiff"}

            timestamped_uris = {uri: timestamp for uri, (timestamp, _) in uris_with_metadata.items()}
            logger.info(f"{len(uris_with_metadata)=}")

            try:
                eo_bands = single_value(eo_bands for _, eo_bands in uris_with_metadata.values())
                band_names = [eo_band["name"] for eo_band in eo_bands]
            except ValueError as e:
                raise OpenEOApiException(message=f"Unsupported band information for job {job_id}: {str(e)}",
                                         status_code=501)

            job_results_bbox = BoundingBox.from_wsen_tuple(
                job_results.extent.spatial.bboxes[0], crs="EPSG:4326"
            )
            # TODO: this assumes that job result data is gridded in best UTM already?
            job_results_epsg = job_results_bbox.best_utm()
            logger.info(f"job result: {job_results_bbox=} {job_results_epsg=}")

        else:
            paths_with_metadata = {
                asset["href"]: (asset.get("datetime"), asset.get("bands", []))
                for _, asset in self.batch_jobs.get_result_assets(
                    job_id=job_id, user_id=user_id
                ).items()
                if asset["type"] == "image/tiff; application=geotiff"
            }
            logger.info(f"{paths_with_metadata=}")

            if len(paths_with_metadata) == 0:
                raise OpenEOApiException(message=f"Job {job_id} contains no results of supported type GTiff.",
                                         status_code=501)

            if not all(timestamp is not None for timestamp, _ in paths_with_metadata.values()):
                raise OpenEOApiException(
                    message=f"Cannot load results of job {job_id} because they lack timestamp information.",
                    status_code=400)

            timestamped_uris = {path: timestamp for path, (timestamp, _) in paths_with_metadata.items()}
            logger.info(f"{timestamped_uris=}")

            try:
                eo_bands = single_value(eo_bands for _, eo_bands in paths_with_metadata.values())
                band_names = [eo_band.name for eo_band in eo_bands]
            except ValueError as e:
                raise OpenEOApiException(message=f"Unsupported band information for job {job_id}: {str(e)}",
                                         status_code=501)

            job_info = self.batch_jobs.get_job_info(job_id, user_id)
            job_results_bbox = BoundingBox.from_wsen_tuple(
                job_info.bbox, crs="EPSG:4326"
            )
            job_results_epsg = job_info.epsg
            logger.info(f"job result: {job_results_bbox=} {job_results_epsg=}")

        metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
            # TODO: detect actual dimensions instead of this simple default?
            SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
            TemporalDimension(name='t', extent=[]),
            BandDimension(name="bands", bands=[Band(band_name) for band_name in band_names])
        ])

        # TODO: eliminate duplication with load_disk_data
        temporal_extent = load_params.temporal_extent
        from_date, to_date = normalize_temporal_extent(temporal_extent)
        metadata = metadata.filter_temporal(from_date, to_date)

        jvm = get_jvm()

        pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_uris(timestamped_uris)

        single_level = env.get('pyramid_levels', 'all') != 'all'

        if single_level:
            target_bbox = requested_bbox or job_results_bbox
            logger.info(f"{target_bbox=}")

            extent = jvm.geotrellis.vector.Extent(*target_bbox.as_wsen_tuple())
            extent_crs = target_bbox.crs

            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(
                extent, target_bbox.crs
            )
            projected_polygons = getattr(
                getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$"
            ).reproject(projected_polygons, job_results_epsg)

            metadata_properties = None
            correlation_id = None
            data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
            getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

            pyramid = pyramid_factory.datacube_seq(projected_polygons, from_date, to_date, metadata_properties,
                                                   correlation_id, data_cube_parameters)
        else:
            if requested_bbox:
                extent = jvm.geotrellis.vector.Extent(*requested_bbox.as_wsen_tuple())
                extent_crs = requested_bbox.crs
            else:
                extent = extent_crs = None

            pyramid = pyramid_factory.pyramid_seq(
                extent, extent_crs, from_date, to_date
            )

        metadata = metadata.filter_bbox(
            west=extent.xmin(),
            south=extent.ymin(),
            east=extent.xmax(),
            north=extent.ymax(),
            crs=extent_crs,
        )

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option

        # noinspection PyProtectedMember
        levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
            option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                  range(0, pyramid.size())}

        cube = GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)

        if load_params.bands:
            cube = cube.filter_bands(load_params.bands)

        return cube

    def load_stac(self, url: str, load_params: LoadParameters, env: EvalEnv) -> GeopysparkDataCube:
        logger.info("load_stac from url {u!r} with load params {p!r}".format(u=url, p=load_params))

        stac_object = pystac.STACObject.from_file(href=url)

        # TODO: specifically handle our own batch job results:
        #  * check if it ends with /jobs/.../results and extract the job ID
        #  * look up the job for the logged-in user and this job ID
        #  * if there is one, just read it directly (you can assume that it belongs to this system (job ID is universally unique) and to this user)
        #  * if there isn't one (catch the exception), treat it like any other STAC (API) URL

        requested_bbox = BoundingBox.from_dict_or_none(
            load_params.spatial_extent, default_crs="EPSG:4326"
        )

        temporal_extent = load_params.temporal_extent
        from_date, to_date = normalize_temporal_extent(temporal_extent)

        def intersects_spatiotemporally(itm: pystac.Item) -> bool:
            def intersects_temporally() -> bool:
                requested_from_date, requested_to_date = normalize_temporal_extent(temporal_extent)
                requested_from_date = datetime.datetime.fromisoformat(requested_from_date)
                requested_to_date = datetime.datetime.fromisoformat(requested_to_date)

                return requested_from_date <= itm.datetime <= requested_to_date

            def intersects_spatially() -> bool:
                if not requested_bbox or itm.bbox is None:
                    return True

                requested_bbox_lonlat = requested_bbox.reproject("EPSG:4326")
                return requested_bbox_lonlat.as_polygon().intersects(
                    Polygon.from_bounds(*itm.bbox)
                )

            return intersects_temporally() and intersects_spatially()

        def is_stac_api(collection: pystac.Collection) -> bool:
            conforms_to = collection.get_root().extra_fields.get("conformsTo", [])
            return any(conformance_class.endswith("/item-search") for conformance_class in conforms_to)

        if isinstance(stac_object, pystac.Item):
            item = stac_object

            if not intersects_spatiotemporally(item):
                raise NoDataAvailableException()

            """
            if load_params.bands:  # can look for specific assets
                band_assets = [(band_name, item.get_assets()[band_name]) for band_name in load_params.bands]
            """
            #else:  # nothing to go by so guess but in a deterministic order
            # TODO: use an OrderedDict instead?
            # TODO: handle assets with multiple bands in them
            sorted_assets = [(asset_id, item.get_assets()[asset_id]) for asset_id in sorted(item.get_assets().keys())]  # TODO: OK to use asset ID because "title" is optional?
            band_assets = [(asset_id, asset) for asset_id, asset in sorted_assets if "eo:bands" in asset.extra_fields]  # FIXME: improve check (but "roles" is optional and its value "data" is only a suggestion)

            jvm = get_jvm()

            opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()
            opensearch_client.addFeature(
                item.id,
                jvm.geotrellis.vector.Extent(*item.bbox),
                rfc3339.datetime(item.datetime.astimezone(datetime.timezone.utc)),
                [[asset.href, asset_id] for asset_id, asset in band_assets]
            )

            band_names = [asset_id for asset_id, _ in band_assets]

            pyramid_factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
                opensearch_client,
                item.id,  # openSearchCollectionId, not important
                band_names,  # openSearchLinkTitles
                None,  # rootPath, not important
                jvm.geotrellis.raster.CellSize(10.0, 10.0),  # TODO, maxSpatialResolution
                False  # experimental
            )

            item_bbox = BoundingBox.from_wsen_tuple(
                item.bbox, crs="EPSG:4326"
            )

            target_bbox = requested_bbox or item_bbox
            target_epsg = target_bbox.best_utm()  # is the default in GeoPySparkLayerCatalog as well

            extent = jvm.geotrellis.vector.Extent(*target_bbox.as_wsen_tuple())
            extent_crs = target_bbox.crs

            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(
                extent, extent_crs
            )
            projected_polygons = getattr(
                getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$"
            ).reproject(projected_polygons, target_epsg)

            metadata_properties = {}
            correlation_id = env.get('correlation_id', '')

            data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
            getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

            pyramid = pyramid_factory.datacube_seq(projected_polygons, from_date, to_date, metadata_properties,
                                                   correlation_id, data_cube_parameters)

            metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
                # TODO: detect actual dimensions instead of this simple default?
                SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
                TemporalDimension(name='t', extent=[]),
                BandDimension(name="bands", bands=[Band(band_name) for band_name in band_names])
            ])

            metadata = metadata.filter_temporal(from_date, to_date)

            metadata = metadata.filter_bbox(
                west=extent.xmin(),
                south=extent.ymin(),
                east=extent.xmax(),
                north=extent.ymax(),
                crs=extent_crs,
            )

            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
            option = jvm.scala.Option

            # noinspection PyProtectedMember
            levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
                option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                      range(0, pyramid.size())}

            cube = GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)

            if load_params.bands:
                cube = cube.filter_bands(load_params.bands)

            return cube
        elif isinstance(stac_object, pystac.Collection) and is_stac_api(stac_object):
            collection = stac_object
            collection_id = collection.id

            root_catalog = collection.get_root()

            jvm = get_jvm()

            band_names = [b["name"] for b in collection.extra_fields.get("summaries", {}).get("eo:bands", [])]

            if not band_names:
                # e.g. https://landsatlook.usgs.gov/stac-server/collections/landsat-c2l2-sr doesn't have this band info
                # (nor a resolution)
                raise FeatureUnsupportedException(
                    "load_stac: collection exposes no band names")  # TODO: different exception?

            def create_stac_api_pyramid_factory():
                is_utm = False
                date_regex = None
                bands = None
                stac_api_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(root_catalog.get_self_href(), is_utm,
                                                                                   date_regex, bands, "stac")

                root_path = None
                cell_size = jvm.geotrellis.raster.CellSize(10.0, 10.0)  # TODO: get it from the band metadata?
                experimental = False
                return jvm.org.openeo.geotrellis.file.PyramidFactory(stac_api_client,
                                                                     collection_id,
                                                                     band_names,
                                                                     root_path,
                                                                     cell_size,
                                                                     experimental)

            single_level = env.get('pyramid_levels', 'all') != 'all'

            if single_level:
                requested_bbox = BoundingBox.from_dict_or_none(
                    load_params.spatial_extent, default_crs="EPSG:4326"
                )
                collection_bbox = BoundingBox.from_wsen_tuple(
                    collection.extent.spatial.bboxes[0], crs="EPSG:4326"
                )

                target_bbox = requested_bbox or collection_bbox
                target_epsg = target_bbox.best_utm()  # is the default in GeoPySparkLayerCatalog as well

                extent = jvm.geotrellis.vector.Extent(*target_bbox.as_wsen_tuple())
                extent_crs = target_bbox.crs

                projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(
                    extent, extent_crs
                )
                projected_polygons = getattr(
                    getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$"
                ).reproject(projected_polygons, target_epsg)

                temporal_extent = load_params.temporal_extent
                from_date, to_date = normalize_temporal_extent(temporal_extent)

                metadata_properties = {}
                correlation_id = env.get('correlation_id', '')

                data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
                getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

                pyramid = create_stac_api_pyramid_factory().datacube_seq(
                    projected_polygons, from_date, to_date,
                    metadata_properties, correlation_id, data_cube_parameters
                )
            else:
                raise NotImplementedError("pyramid")

            metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
                # TODO: detect actual dimensions instead of this simple default?
                SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
                TemporalDimension(name='t', extent=[]),
                BandDimension(name="bands", bands=[Band(band_name) for band_name in band_names])
            ])

            metadata = metadata.filter_temporal(from_date, to_date)

            metadata = metadata.filter_bbox(
                west=extent.xmin(),
                south=extent.ymin(),
                east=extent.xmax(),
                north=extent.ymax(),
                crs=extent_crs,
            )

            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
            option = jvm.scala.Option

            # noinspection PyProtectedMember
            levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
                option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                      range(0, pyramid.size())}

            cube = GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)

            if load_params.bands:
                cube = cube.filter_bands(load_params.bands)

            return cube
        else:
            assert isinstance(stac_object, pystac.Catalog)
            catalog = stac_object

            intersecting_items = [itm for itm in catalog.get_all_items() if intersects_spatiotemporally(itm)]

            if len(intersecting_items) == 0:
                raise NoDataAvailableException()

            jvm = get_jvm()

            opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()

            band_names = []
            catalog_bbox = None

            for itm in intersecting_items:
                sorted_assets = [(asset_id, itm.get_assets()[asset_id]) for asset_id in sorted(itm.get_assets().keys())]
                band_assets = [(asset_id, asset) for asset_id, asset in sorted_assets if "eo:bands" in asset.extra_fields]

                def get_band_names(asset: pystac.Asset) -> List[str]:
                    def get_band_name(eo_band) -> str:
                        if isinstance(eo_band, dict):
                            return eo_band["name"]

                        # can also be an index into a list of bands elsewhere
                        eo_bands_location = itm.properties if "eo:bands" in itm.properties else itm.collection.summaries
                        return get_band_name(eo_bands_location["eo:bands"][eo_band])

                    return [get_band_name(eo_band) for eo_band in asset.extra_fields["eo:bands"]]

                links = []
                for asset_id, asset in band_assets:
                    asset_band_names = get_band_names(asset)
                    for asset_band_name in asset_band_names:
                        if asset_band_name not in band_names:
                            band_names.append(asset_band_name)

                    links.append([asset.href, asset_id] + asset_band_names)

                opensearch_client.addFeature(
                    itm.id,
                    jvm.geotrellis.vector.Extent(*itm.bbox),
                    rfc3339.datetime(itm.datetime.astimezone(datetime.timezone.utc)),
                    links
                )

                item_bbox = BoundingBox.from_wsen_tuple(
                    itm.bbox, crs="EPSG:4326"
                )

                catalog_bbox = (item_bbox if catalog_bbox is None
                                else item_bbox.as_polygon().union(catalog_bbox.as_polygon()))

            pyramid_factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
                opensearch_client,
                catalog.id,  # openSearchCollectionId, not important
                band_names,  # openSearchLinkTitles
                None,  # rootPath, not important
                jvm.geotrellis.raster.CellSize(10.0, 10.0),  # TODO, maxSpatialResolution
                False  # experimental
            )

            target_bbox = requested_bbox or catalog_bbox
            target_epsg = target_bbox.best_utm()  # is the default in GeoPySparkLayerCatalog as well

            extent = jvm.geotrellis.vector.Extent(*target_bbox.as_wsen_tuple())
            extent_crs = target_bbox.crs

            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(
                extent, extent_crs
            )
            projected_polygons = getattr(
                getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$"
            ).reproject(projected_polygons, target_epsg)

            metadata_properties = {}
            correlation_id = env.get('correlation_id', '')

            data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
            getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

            pyramid = pyramid_factory.datacube_seq(projected_polygons, from_date, to_date, metadata_properties,
                                                   correlation_id, data_cube_parameters)

            metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
                # TODO: detect actual dimensions instead of this simple default?
                SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
                TemporalDimension(name='t', extent=[]),
                BandDimension(name="bands", bands=[Band(band_name) for band_name in band_names])
            ])

            metadata = metadata.filter_temporal(from_date, to_date)

            metadata = metadata.filter_bbox(
                west=extent.xmin(),
                south=extent.ymin(),
                east=extent.xmax(),
                north=extent.ymax(),
                crs=extent_crs,
            )

            temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
            option = jvm.scala.Option

            # noinspection PyProtectedMember
            levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
                option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                      range(0, pyramid.size())}

            cube = GeopysparkDataCube(pyramid=gps.Pyramid(levels), metadata=metadata)

            if load_params.bands:
                cube = cube.filter_bands(load_params.bands)

            return cube

    def load_ml_model(self, model_id: str) -> 'JavaObject':

        # Trick to make sure IDE infers right type of `self.batch_jobs` and can resolve `get_job_output_dir`
        gps_batch_jobs: GpsBatchJobs = self.batch_jobs

        def _create_model_dir():
            def _set_permissions(job_dir: Path):
                if not ConfigParams().is_kube_deploy:
                    try:
                        shutil.chown(job_dir, user = None, group = 'eodata')
                    except LookupError as e:
                        logger.warning(f"Could not change group of {job_dir} to eodata.")
                add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group
            ml_models_path = gps_batch_jobs.get_job_output_dir("ml_models")
            if not os.path.exists(ml_models_path):
                logger.info("Creating directory: {}".format(ml_models_path))
                os.makedirs(ml_models_path)
                _set_permissions(ml_models_path)
            # Use a random id to avoid collisions.
            model_dir_path = ml_models_path / generate_unique_id(prefix="model")
            if not os.path.exists(model_dir_path):
                logger.info("Creating directory: {}".format(model_dir_path))
                os.makedirs(model_dir_path)
                _set_permissions(model_dir_path)
            return str(model_dir_path)

        if model_id.startswith('http'):
            # Load the model using its STAC metadata file.
            metadata = requests.get(model_id).json()
            if deep_get(metadata, "properties", "ml-model:architecture", default=None) is None:
                raise OpenEOApiException(
                    message=f"{model_id} does not specify a model architecture under properties.ml-model:architecture.",
                    status_code=400)
            checkpoints = []
            assets = metadata.get('assets', {})
            for asset in assets:
                if "ml-model:checkpoint" in assets[asset].get('roles', []):
                    checkpoints.append(assets[asset])
            if len(checkpoints) == 0 or checkpoints[0].get("href", None) is None:
                raise OpenEOApiException(
                    message=f"{model_id} does not contain a link to the ml model in its assets section.",
                    status_code=400)
            # TODO: How to handle multiple models?
            if len(checkpoints) > 1:
                raise OpenEOApiException(
                    message=f"{model_id} contains multiple checkpoints.",
                    status_code=400)

            # Get the url for the actual model from the STAC metadata.
            model_url = checkpoints[0]["href"]
            architecture = metadata["properties"]["ml-model:architecture"]
            # Download the model to the ml_models folder and load it as a java object.
            model_dir_path = _create_model_dir()
            if architecture == "random-forest":
                dest_path = Path(model_dir_path + "/randomforest.model.tar.gz")
                with open(dest_path, 'wb') as f:
                    f.write(requests.get(model_url).content)
                shutil.unpack_archive(dest_path, extract_dir=model_dir_path, format='gztar')
                unpacked_model_path = str(dest_path).replace(".tar.gz", "")
                logger.info("Loading ml_model using filename: {}".format(unpacked_model_path))
                model: JavaObject = RandomForestModel._load_java(sc=gps.get_spark_context(), path="file:" + unpacked_model_path)
            elif architecture == "catboost":
                filename = Path(model_dir_path + "/catboost_model.cbm")
                with open(filename, 'wb') as f:
                    f.write(requests.get(model_url).content)
                logger.info("Loading ml_model using filename: {}".format(filename))
                model: JavaObject = CatBoostClassificationModel.load_native_model(str(filename))
            else:
                raise NotImplementedError("The ml-model architecture is not supported by the backend: " + architecture)
            return model
        else:
            # Load the model using a batch job id.
            directory = gps_batch_jobs.get_job_output_dir(model_id)
            # TODO: This also needs to support Catboost model
            # TODO: This can be done by first reading ml_model_metadata.json in the batch job directory.
            model_path = str(Path(directory) / "randomforest.model")
            if Path(model_path).exists():
                logger.info("Loading ml_model using filename: {}".format(model_path))
                model: JavaObject = RandomForestModel._load_java(sc=gps.get_spark_context(), path="file:" + model_path)
            elif Path(model_path+".tar.gz").exists():
                packed_model_path = model_path+".tar.gz"
                shutil.unpack_archive(packed_model_path, extract_dir=directory, format='gztar')
                unpacked_model_path = str(packed_model_path).replace(".tar.gz", "")
                model: JavaObject = RandomForestModel._load_java(sc=gps.get_spark_context(), path="file:" + unpacked_model_path)
            else:
                raise OpenEOApiException(
                    message=f"No random forest model found for job {model_id}",status_code=400)
            return model

    def visit_process_graph(self, process_graph: dict) -> ProcessGraphVisitor:
        return GeoPySparkBackendImplementation.accept_process_graph(process_graph)

    @classmethod
    def accept_process_graph(cls, process_graph):
        if len(process_graph) == 1 and next(iter(process_graph.values())).get('process_id') == 'run_udf':
            return SingleNodeUDFProcessGraphVisitor().accept_process_graph(process_graph)
        return GeotrellisTileProcessGraphVisitor().accept_process_graph(process_graph)

    def summarize_exception(self, error: Exception) -> Union[ErrorSummary, Exception]:
        return self.summarize_exception_static(error, 2000)

    @staticmethod
    def summarize_exception_static(error: Exception, width=2000) -> Union[ErrorSummary, Exception]:
        if "Container killed on request. Exit code is 143" in str(error):
            is_client_error = False  # Give user the benefit of doubt.
            summary = "Your batch job failed because workers used too much Python memory. The same task was attempted multiple times. Consider increasing executor-memoryOverhead or contact the developers to investigate."

        elif isinstance(error, Py4JJavaError):
            java_exception = error.java_exception

            java_exception_class_name = java_exception.getClass().getName()
            is_spark_exception = "SparkException" in java_exception_class_name
            while java_exception.getCause() is not None and java_exception != java_exception.getCause():
                java_exception = java_exception.getCause()

            java_exception_class_name = java_exception.getClass().getName()
            java_exception_message = java_exception.getMessage()

            no_data_found = (java_exception_class_name == 'java.lang.AssertionError'
                             and "Cannot stitch empty collection" in java_exception_message)

            is_client_error = java_exception_class_name == 'java.lang.IllegalArgumentException' or no_data_found
            if no_data_found:
                summary = "Cannot construct an image because the given boundaries resulted in an empty image collection"
            elif is_spark_exception:
                udf_stacktrace = GeoPySparkBackendImplementation.extract_udf_stacktrace(java_exception_message)
                if udf_stacktrace:
                    summary = f"UDF Exception during Spark execution: {udf_stacktrace}"
                else:
                    summary = f"Exception during Spark execution: {java_exception_message}"
            else:
                summary = java_exception_class_name + ": " + str(java_exception_message)
            summary = str_truncate(summary, width=width)
        else:
            is_client_error = False  # Give user the benefit of doubt.
            summary = repr_truncate(error, width=width)

        return ErrorSummary(error, is_client_error, summary)

    @staticmethod
    def extract_udf_stacktrace(full_stacktrace) -> Optional[str]:
        """
        Select all lines a bit under 'run_udf_code'.
        This is what interests the user
        """
        regex = re.compile(r" in run_udf_code\n.*\n((.|\n)*)", re.MULTILINE)

        match = regex.search(full_stacktrace)
        if match:
            return match.group(1).rstrip()
        return None

    def changelog(self) -> Union[str, Path]:
        roots = []
        if Path(__file__).parent.parent.name == "openeo-geopyspark-driver":
            # Local dev path
            roots.append(Path(__file__).parent.parent)
        # Installed package/wheel location
        roots.append(Path(sys.prefix) / "openeo-geopyspark-driver")
        for root in roots:
            if (root / "CHANGELOG.md").exists():
                return root / "CHANGELOG.md"

        return super().changelog()

    def set_request_id(self, request_id: str):
        sc = SparkContext.getOrCreate()
        jvm = sc._gateway.jvm

        mdc_include(sc, jvm, jvm.org.openeo.logging.JsonLayout.RequestId(), request_id)

    def user_access_validation(self, user: User, request: flask.Request) -> User:
        # TODO: add dedicated method instead of abusing this one?
        sc = SparkContext.getOrCreate()
        jvm = sc._gateway.jvm
        user_id = user.user_id

        mdc_include(sc, jvm, jvm.org.openeo.logging.JsonLayout.UserId(), user_id)

        return user

    def after_request(self, request_id: str):
        sc = SparkContext.getOrCreate()
        jvm = sc._gateway.jvm

        jvm.org.openeo.geotrelliscommon.ScopedMetadataTracker.remove(request_id)

        for mdc_key in [jvm.org.openeo.logging.JsonLayout.RequestId(), jvm.org.openeo.logging.JsonLayout.UserId()]:
            mdc_remove(sc, jvm, mdc_key)

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self.batch_jobs.set_default_sentinel_hub_credentials(client_id, client_secret)
        self.catalog.set_default_sentinel_hub_credentials(client_id, client_secret)

    def set_terrascope_access_token_getter(self, get_terrascope_access_token: Callable[[User, str], str]):
        self.batch_jobs.set_terrascope_access_token_getter(get_terrascope_access_token)

    def request_costs(self, user_id: str, request_id: str, success: bool) -> Optional[float]:
        return self._get_request_costs(user_id, request_id, success)


class GpsProcessing(ConcreteProcessing):
    def extra_validation(
            self, process_graph: dict, env: EvalEnv, result, source_constraints: List[SourceConstraint]
    ) -> Iterable[dict]:

        catalog = env.backend_implementation.catalog

        for source_id, constraints in source_constraints:
            source_id_proc, source_id_args = source_id
            if source_id_proc == "load_collection":
                collection_id = source_id_args[0]
                metadata = GeopysparkCubeMetadata(catalog.get_collection_metadata(collection_id=collection_id))
                temporal_extent = constraints.get("temporal_extent")
                spatial_extent = constraints.get("spatial_extent")

                if metadata.get("_vito", "data_source", "check_missing_products", default=None):
                    properties = constraints.get("properties", {})
                    if temporal_extent is None:
                        yield {"code": "UnlimitedExtent", "message": "No temporal extent given."}
                    if spatial_extent is None:
                        yield {"code": "UnlimitedExtent", "message": "No spatial extent given."}
                    if temporal_extent is None or spatial_extent is None:
                        return

                    products = check_missing_products(
                        collection_metadata=metadata,
                        temporal_extent=temporal_extent,
                        spatial_extent=spatial_extent,
                        properties=properties,
                    )
                    if products:
                        for p in products:
                            yield {
                                "code": "MissingProduct",
                                "message": f"Tile {p!r} in collection {collection_id!r} is not available."
                            }

                cell_width = float(metadata.get("cube:dimensions", "x", "step", default = 10.0))
                cell_height = float(metadata.get("cube:dimensions", "y", "step", default = 10.0))
                native_crs = metadata.get("cube:dimensions", "x", "reference_system", default = "EPSG:4326")
                if isinstance(native_crs, dict):
                    native_crs = native_crs.get("id", {}).get("code", None)
                if isinstance(native_crs, int):
                    native_crs = f"EPSG:{native_crs}"
                if not isinstance(native_crs, str):
                    yield {"code": "InvalidNativeCRS", "message": f"Invalid native CRS {native_crs!r} for "
                                                                  f"collection {collection_id!r}"}
                    continue

                bands = constraints.get("bands", [])
                geometries = constraints.get("aggregate_spatial", {}).get("geometries")
                if geometries is None:
                    geometries = constraints.get("filter_spatial", {}).get("geometries")
                too_large, estimated_pixels, threshold_pixels = is_layer_too_large(
                    spatial_extent=spatial_extent,
                    geometries=geometries,
                    temporal_extent=temporal_extent,
                    nr_bands=len(bands),
                    cell_width=cell_width,
                    cell_height=cell_height,
                    native_crs=native_crs,
                    resample_params=constraints.get("resample", {}),
                )
                if too_large:
                    yield {
                        "code": "ExtentTooLarge",
                        "message": f"Requested extent for collection {collection_id!r} is too large to process. "
                                   f"Estimated number of pixels: {estimated_pixels:.2e}, "
                                   f"threshold: {threshold_pixels:.2e}."
                    }

    def run_udf(self, udf: str, data: openeo.udf.UdfData) -> openeo.udf.UdfData:
        if get_backend_config().allow_run_udf_in_driver:
            # TODO: remove this temporary feature flag https://github.com/Open-EO/openeo-geopyspark-driver/issues/404
            return run_udf_code(code=udf, data=data, require_executor_context=False)
        sc = SparkContext.getOrCreate()
        data_rdd = sc.parallelize([data])
        result_rdd = data_rdd.map(lambda d: run_udf_code(code=udf, data=d))
        result = result_rdd.collect()
        if not len(result) == 1:
            raise InternalException(message=f"run_udf result RDD with 1 element expected but got {len(result)}")
        return result[0]


def get_elastic_job_registry(
    requests_session: Optional[requests.Session] = None,
) -> Optional[ElasticJobRegistry]:
    """Build ElasticJobRegistry instance from config and vault"""
    with ElasticJobRegistry.just_log_errors(name="get_elastic_job_registry"):
        config = ConfigParams()
        job_registry = ElasticJobRegistry(
            api_url=config.ejr_api,
            backend_id=config.ejr_backend_id,
            session=requests_session,
        )
        vault = Vault(config.vault_addr, requests_session=requests_session)
        ejr_creds = vault.get_elastic_job_registry_credentials()
        job_registry.setup_auth_oidc_client_credentials(credentials=ejr_creds)
        job_registry.health_check(log=True)
        return job_registry


class GpsBatchJobs(backend.BatchJobs):

    def __init__(
        self,
        catalog: GeoPySparkLayerCatalog,
        jvm: JVMView,
        principal: str,
        key_tab: str,
        vault: Vault,
        output_root_dir: Optional[Union[str, Path]] = None,
        elastic_job_registry: Optional[ElasticJobRegistry] = None,
    ):
        super().__init__()
        self._catalog = catalog
        self._jvm = jvm
        self._principal = principal
        self._key_tab = key_tab
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None
        self._get_terrascope_access_token: Optional[Callable[[User, str], str]] = None
        self._vault = vault

        # TODO: Generalize assumption that output_dir == local FS? (e.g. results go to non-local S3)
        self._output_root_dir = Path(
            output_root_dir or ConfigParams().batch_job_output_root
        )

        self._double_job_registry = DoubleJobRegistry(
            zk_job_registry_factory=ZkJobRegistry,
            elastic_job_registry=elastic_job_registry,
        )

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    def set_terrascope_access_token_getter(self, get_terrascope_access_token: Callable[[User, str], str]):
        self._get_terrascope_access_token = get_terrascope_access_token

    def create_job(
        self,
        user_id: str,
        process: dict,
        api_version: str,
        metadata: dict,
        job_options: Optional[dict] = None,
    ) -> BatchJobMetadata:
        job_id = generate_unique_id(prefix="j")
        title = metadata.get("title")
        description = metadata.get("description")
        with self._double_job_registry as registry:
            job_info = registry.create_job(
                job_id=job_id,
                user_id=user_id,
                process=process,
                api_version=api_version,
                job_options=job_options,
                title=title,
                description=description,
            )
        return BatchJobMetadata(
            id=job_id, process=process, status=job_info["status"],
            created=rfc3339.parse_datetime(job_info["created"]), job_options=job_options,
            title=title, description=description,
        )

    def get_job_info(self, job_id: str, user_id: str) -> BatchJobMetadata:
        with self._double_job_registry as registry:
            job_info = registry.get_job(job_id, user_id)

        return zk_job_info_to_metadata(job_info)

    def poll_sentinelhub_batch_processes(
        self,
        job_info: dict,
        sentinel_hub_client_alias: str,
        vault_token: Optional[str] = None,
    ):
        # TODO: split polling logic and resuming logic?
        job_id, user_id = job_info['job_id'], job_info['user_id']

        def batch_request_details(batch_process_dependency: dict) -> Dict[str, Tuple[str, Callable[[], None]]]:
            """returns an ID -> (status, retrier) for each batch request ID in the dependency"""
            collection_id = batch_process_dependency['collection_id']

            metadata = GeopysparkCubeMetadata(self._catalog.get_collection_metadata(collection_id))
            temporal_step = metadata.get("cube:dimensions", "t", "step")
            layer_source_info = metadata.get("_vito", "data_source", default={})

            endpoint = layer_source_info['endpoint']
            bucket_name = layer_source_info.get('bucket', sentinel_hub.OG_BATCH_RESULTS_BUCKET)

            logger.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}", extra={'job_id': job_id,
                                                                                           'user_id': user_id})

            if sentinel_hub_client_alias == 'default':
                sentinel_hub_client_id = self._default_sentinel_hub_client_id
                sentinel_hub_client_secret = self._default_sentinel_hub_client_secret
            else:
                sentinel_hub_client_id, sentinel_hub_client_secret = (
                    self._vault.get_sentinel_hub_credentials(sentinel_hub_client_alias, vault_token))

            batch_processing_service = (
                SentinelHubBatchProcessing.get_batch_processing_service(
                    endpoint=endpoint,
                    bucket_name=bucket_name,
                    sentinel_hub_client_id=sentinel_hub_client_id,
                    sentinel_hub_client_secret=sentinel_hub_client_secret,
                    sentinel_hub_client_alias=sentinel_hub_client_alias,
                    jvm=self._jvm,
                )
            )

            batch_request_ids = (batch_process_dependency.get('batch_request_ids') or
                                 [batch_process_dependency['batch_request_id']])

            def retrier(request_id: str) -> Callable[[], None]:
                def retry():
                    assert request_id is not None, "retry is for PARTIAL statuses but a 'None' request_id is DONE"

                    logger.warning(f"retrying Sentinel Hub batch process {request_id} for batch job {job_id}",
                                   extra={'job_id': job_id, 'user_id': user_id})
                    batch_processing_service.restart_partially_failed_batch_process(request_id)

                return retry

            # TODO: prevent requests for duplicate (recycled) batch request IDs
            return {request_id: (batch_processing_service.get_batch_process(request_id), temporal_step,
                                 retrier(request_id)) for request_id in batch_request_ids if request_id is not None}

        dependencies = job_info.get('dependencies') or []
        batch_processes = reduce(partial(dict_merge_recursive, overwrite=True),
                                 (batch_request_details(dependency) for dependency in dependencies))

        batch_process_statuses = {batch_request_id: details.status()
                                  for batch_request_id, (details, _, _) in batch_processes.items()}

        logger.debug("Sentinel Hub batch process statuses for batch job {j}: {ss}"
                     .format(j=job_id, ss=batch_process_statuses), extra={'job_id': job_id, 'user_id': user_id})

        if any(status == "FAILED" for status in batch_process_statuses.values()):  # at least one failed: not recoverable
            batch_process_errors = {batch_request_id: details.errorMessage() or "<no error details>"
                                    for batch_request_id, (details, _, _) in batch_processes.items()
                                    if details.status() == "FAILED"}

            logger.error(f"Failing batch job because one or more Sentinel Hub batch processes failed: "
                         f"{batch_process_errors}", extra={'job_id': job_id, 'user_id': user_id})

            with self._double_job_registry as registry:
                registry.set_dependency_status(job_id, user_id, DEPENDENCY_STATUS.ERROR)
                registry.set_status(job_id, user_id, JOB_STATUS.ERROR)

            job_info["status"] = JOB_STATUS.ERROR  # TODO: avoid mutation
        elif all(status == "DONE" for status in batch_process_statuses.values()):  # all good: resume batch job with available data
            assembled_location_cache = {}

            for dependency in dependencies:
                collecting_folder = dependency.get('collecting_folder')

                if collecting_folder:  # the collection is at least partially cached
                    assembled_location = assembled_location_cache.get(collecting_folder)

                    if assembled_location is None:
                        caching_service = self._jvm.org.openeo.geotrellissentinelhub.CachingService()

                        results_location = dependency.get('results_location')

                        if results_location is not None:
                            uri_parts = urlparse(results_location)
                            bucket_name = uri_parts.hostname
                            subfolder = uri_parts.path[1:]
                        else:
                            bucket_name = sentinel_hub.OG_BATCH_RESULTS_BUCKET
                            subfolder = dependency['subfolder']

                        caching_service.download_and_cache_results(bucket_name, subfolder, collecting_folder)

                        # assembled_folder must be readable from batch job driver (load_collection)
                        assembled_folder = f"/tmp_epod/openeo_assembled/{generate_unique_id()}"
                        os.mkdir(assembled_folder)
                        os.chmod(assembled_folder, mode=0o750)  # umask prevents group read

                        caching_service.assemble_multiband_tiles(collecting_folder, assembled_folder, bucket_name,
                                                                 subfolder)

                        assembled_location = f"file://{assembled_folder}/"
                        assembled_location_cache[collecting_folder] = assembled_location

                        logger.debug("saved new assembled location {a} for near future use (key {k!r})"
                                     .format(a=assembled_location, k=collecting_folder), extra={'job_id': job_id,
                                                                                                'user_id': user_id})

                        try:
                            # TODO: if the subsequent spark-submit fails, the collecting_folder is gone so this job
                            #  can't be recovered by fiddling with its dependency_status.
                            shutil.rmtree(collecting_folder)
                        except Exception as e:
                            logger.warning("Could not recursively delete {p}".format(p=collecting_folder), exc_info=e,
                                           extra={'job_id': job_id, 'user_id': user_id})
                    else:
                        logger.debug("recycling saved assembled location {a} (key {k!r})".format(
                            a=assembled_location, k=collecting_folder), extra={'job_id': job_id, 'user_id': user_id})

                    dependency['assembled_location'] = assembled_location
                else:  # no caching involved, the collection is fully defined by these batch process results
                    pass

            with self._double_job_registry as registry:
                def processing_units_spent(value_estimate: Decimal, temporal_step: Optional[str]) -> Decimal:
                    seconds_per_day = 24 * 3600
                    temporal_interval_in_days: Optional[float] = (
                        None if temporal_step is None else Timedelta(temporal_step).total_seconds() / seconds_per_day)

                    default_temporal_interval = 3
                    estimate_to_pu_ratio = 3
                    estimate_secure_factor = 2
                    temporal_interval = Decimal(temporal_interval_in_days or default_temporal_interval)
                    return (value_estimate * estimate_secure_factor / estimate_to_pu_ratio * default_temporal_interval
                            / temporal_interval)

                batch_process_processing_units = sum(processing_units_spent(details.value_estimate() or Decimal("0.0"),
                                                                            temporal_step)
                                                     for details, temporal_step, _ in batch_processes.values())

                logger.debug(f"Total cost of Sentinel Hub batch processes: {batch_process_processing_units} PU",
                             extra={'job_id': job_id, 'user_id': user_id})

                registry.set_dependencies(job_id, user_id, dependencies)
                registry.set_dependency_status(job_id, user_id, DEPENDENCY_STATUS.AVAILABLE)
                registry.set_dependency_usage(job_id, user_id, batch_process_processing_units)

            self._start_job(job_id, user_id, lambda _: vault_token, dependencies)
        elif all(
            status in ["DONE", "PARTIAL"] for status in batch_process_statuses.values()
        ):  # all done but some partially failed
            if (
                job_info.get("dependency_status") != DEPENDENCY_STATUS.AWAITING_RETRY
            ):  # haven't retried yet: retry
                with self._double_job_registry as registry:
                    registry.set_dependency_status(job_id, user_id, DEPENDENCY_STATUS.AWAITING_RETRY)

                retries = [retry for details, _, retry in batch_processes.values() if details.status() == "PARTIAL"]

                for retry in retries:
                    retry()
                # TODO: the assumption is that a successful /restartpartial request means that processing has
                #  effectively restarted and a different status (PROCESSING) is published; otherwise the next poll might
                #  still see the previous status (PARTIAL), consider it the new status and immediately mark it as
                #  unrecoverable.
            else:  # still some PARTIALs after one retry: not recoverable
                logger.error(f"Retrying did not fix PARTIAL Sentinel Hub batch processes, aborting job: "
                             f"{batch_process_statuses}", extra={'job_id': job_id, 'user_id': user_id})

                with self._double_job_registry as registry:
                    registry.set_dependency_status(job_id, user_id, DEPENDENCY_STATUS.ERROR)
                    registry.set_status(job_id, user_id, JOB_STATUS.ERROR)

                job_info["status"] = JOB_STATUS.ERROR  # TODO: avoid mutation
        else:  # still some in progress and none FAILED yet: continue polling
            pass

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        with self._double_job_registry as registry:
            return registry.get_user_jobs(user_id=user_id)

    # TODO: issue #232 we should get this from S3 but should there still be an output dir then?
    def get_job_output_dir(self, job_id: str) -> Path:
        # TODO: instead of flat dir with potentially a lot of subdirs (which is hard to maintain/clean up):
        #       add an intermediate level (e.g. based on job_id/user_id prefix or date or ...)?
        return self._output_root_dir / job_id

    @staticmethod
    def get_submit_py_files(env: dict = None, cwd: Union[str, Path] = ".") -> str:
        """Get `-py-files` for batch job submit (e.g. based on how flask app was submitted)."""
        py_files = (env or os.environ).get("OPENEO_SPARK_SUBMIT_PY_FILES", "")
        cwd = Path(cwd)
        if py_files:
            found = []
            # Spark-submit moves `py-files` directly into job folder (`cwd`),
            # or under __pyfiles__ subfolder in case of *.py, regardless of original path.
            for filename in (Path(p).name for p in py_files.split(",")):
                if (cwd / filename).exists():
                    found.append(filename)
                elif (cwd / "__pyfiles__" / filename).exists():
                    found.append("__pyfiles__/" + filename)
                else:
                    logger.warning(f"Could not find 'py-file' {filename}: skipping")
            py_files = ",".join(found)
        return py_files

    def start_job(self, job_id: str, user: User):
        proxy_user = self.get_proxy_user(user)
        logger.info(f"Starting job {job_id!r} from user {user!r} (proxy user {proxy_user!r})",
                    extra={'job_id': job_id, 'user_id': user.user_id})

        if proxy_user:
            with self._double_job_registry as registry:
                registry.set_proxy_user(
                    job_id=job_id, user_id=user.user_id, proxy_user=proxy_user
                )

        # only fetch it when necessary (SHub collection with non-default credentials) and only once
        @lru_cache(maxsize=None)
        def _get_vault_token(sentinel_hub_client_alias: str) -> str:
            terrascope_access_token = self._get_terrascope_access_token(user, sentinel_hub_client_alias)
            return self._vault.login_jwt(terrascope_access_token)

        self._start_job(job_id, user.user_id, _get_vault_token)

    def _start_job(self, job_id: str, user_id: str, get_vault_token: Callable[[str], str],
                   batch_process_dependencies: Union[list, None] = None):
        from openeogeotrellis import async_task  # TODO: avoid local import because of circular dependency

        with self._double_job_registry as dbl_registry:
            job_info = dbl_registry.get_job(job_id, user_id)
            api_version = job_info.get('api_version')

            if batch_process_dependencies is None:
                # restart logic
                current_status = job_info['status']

                if current_status in [JOB_STATUS.QUEUED, JOB_STATUS.RUNNING]:
                    return
                elif current_status != JOB_STATUS.CREATED:  # TODO: not in line with the current spec (it must first be canceled)
                    dbl_registry.mark_ongoing(job_id, user_id)
                    dbl_registry.set_application_id(job_id, user_id, None)
                    dbl_registry.set_status(job_id, user_id, JOB_STATUS.CREATED)

            spec = json.loads(job_info['specification'])
            job_title = job_info.get('title', '')
            job_options = spec.get('job_options', {})
            sentinel_hub_client_alias = deep_get(job_options, 'sentinel-hub', 'client-alias', default="default")

            logger.debug("job_options: {o!r}".format(o=job_options), extra={'job_id': job_id, 'user_id': user_id})

            if (
                batch_process_dependencies is None
                and job_info.get("dependency_status")
                not in [
                    DEPENDENCY_STATUS.AWAITING,
                    DEPENDENCY_STATUS.AWAITING_RETRY,
                    DEPENDENCY_STATUS.AVAILABLE,
                ]
                and self._scheduled_sentinelhub_batch_processes(
                    process_graph=spec["process_graph"],
                    api_version=api_version,
                    dbl_registry=dbl_registry,
                    user_id=user_id,
                    job_id=job_id,
                    job_options=job_options,
                    sentinel_hub_client_alias=sentinel_hub_client_alias,
                    get_vault_token=get_vault_token,
                )
            ):
                async_task.schedule_poll_sentinelhub_batch_processes(
                    batch_job_id=job_id,
                    user_id=user_id,
                    sentinel_hub_client_alias=sentinel_hub_client_alias,
                    vault_token=None
                    if sentinel_hub_client_alias == "default"
                    else get_vault_token(sentinel_hub_client_alias),
                )
                dbl_registry.set_dependency_status(
                    job_id, user_id, DEPENDENCY_STATUS.AWAITING
                )
                dbl_registry.set_status(job_id, user_id, JOB_STATUS.QUEUED)

                return

            def as_boolean_arg(job_option_key: str, default_value: str) -> str:
                value = job_options.get(job_option_key)

                if value is None:
                    return default_value
                elif isinstance(value, str):
                    return value
                elif isinstance(value, bool):
                    return str(value).lower()

                raise OpenEOApiException(f"invalid value {value} for job_option {job_option_key}")

            def as_max_soft_errors_ratio_arg() -> str:
                value = job_options.get("soft-errors")

                if value in [None, "false"]:
                    return "0.0"
                elif value == "true":
                    return "1.0"
                elif isinstance(value, bool):
                    return "1.0" if value else "0.0"
                elif isinstance(value, (int, float)) and 0.0 <= value <= 1.0:
                    return str(value)

                raise OpenEOApiException(message=f"invalid value {value} for job_option soft-errors; "
                                                 f"supported values include false/true and values in the "
                                                 f"interval [0.0, 1.0]",
                                         status_code=400)

            def as_logging_threshold_arg() -> str:
                value = job_options.get("logging-threshold", "info").upper()

                if value == "WARNING":
                    value = "WARN"  # Log4j only accepts WARN whereas Python logging accepts WARN as well as WARNING

                if value in ["DEBUG", "INFO", "WARN", "ERROR"]:
                    return value

                raise OpenEOApiException(message=f"invalid value {value} for job_option logging-threshold; "
                                                 f'supported values include "debug", "info", "warning" and "error"',
                                         status_code=400)

            isKube = ConfigParams().is_kube_deploy
            driver_memory = job_options.get("driver-memory", "2G" if isKube else "8G" )
            driver_memory_overhead = job_options.get("driver-memoryOverhead", "1G" if isKube else "2G")
            executor_memory = job_options.get("executor-memory", "2G")
            executor_memory_overhead = job_options.get("executor-memoryOverhead", "1800m" if isKube else "3G")
            driver_cores = str(job_options.get("driver-cores", 1 if isKube else 5))
            executor_cores = str(job_options.get("executor-cores", 1 if isKube else 2))
            executor_corerequest = job_options.get("executor-request-cores", "NONE")
            if executor_corerequest == "NONE":
                executor_corerequest = str(int(executor_cores)/2*1000)+"m"
            max_executors = str(job_options.get("max-executors", 20 if isKube else 100))
            executor_threads_jvm = str(job_options.get("executor-threads-jvm", 8 if isKube else 10))
            queue = job_options.get("queue", "default")
            profile = as_boolean_arg("profile", default_value="false")
            max_soft_errors_ratio = as_max_soft_errors_ratio_arg()
            task_cpus = str(job_options.get("task-cpus", 1))
            archives = ",".join(job_options.get("udf-dependency-archives", []))
            use_goofys = as_boolean_arg("goofys", default_value="false") != "false"
            mount_tmp = as_boolean_arg("mount_tmp", default_value="false") != "false"
            use_pvc = as_boolean_arg("spark_pvc", default_value="false") != "false"
            logging_threshold = as_logging_threshold_arg()

            def serialize_dependencies() -> str:
                dependencies = batch_process_dependencies or job_info.get('dependencies') or []

                def as_arg_element(dependency: dict) -> dict:
                    source_location = (dependency.get('assembled_location')  # cached
                                       or dependency.get('results_location')  # not cached
                                       or f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}/{dependency.get('subfolder') or dependency['batch_request_id']}")  # legacy

                    return {
                        'source_location': source_location,
                        'card4l': dependency.get('card4l', False)
                    }

                return json.dumps([as_arg_element(dependency) for dependency in dependencies])


            if not isKube:
                kerberos(self._principal, self._key_tab, self._jvm)

            if isKube:
                import yaml
                import time
                import io

                from jinja2 import Environment, FileSystemLoader
                from kubernetes.client.rest import ApiException

                bucket = ConfigParams().s3_bucket_name
                s3_instance = s3_client()

                s3_instance.create_bucket(Bucket=bucket)

                output_dir = str(self.get_job_output_dir(job_id))

                job_specification_file = output_dir + '/job_specification.json'

                jobspec_bytes = str.encode(job_info['specification'])
                file = io.BytesIO(jobspec_bytes)
                s3_instance.upload_fileobj(file, bucket, job_specification_file.strip('/'))

                if api_version:
                    api_version = api_version
                else:
                    api_version = '0.4.0'

                memOverheadBytes = self._jvm.org.apache.spark.util.Utils.byteStringAsBytes(executor_memory_overhead)
                jvmOverheadBytes = self._jvm.org.apache.spark.util.Utils.byteStringAsBytes("128m")
                python_max = memOverheadBytes - jvmOverheadBytes

                eodata_mount = "/eodata2" if use_goofys else "/eodata"

                jinja_path = pkg_resources.resource_filename(
                    "openeogeotrellis.deploy", "sparkapplication.yaml.j2"
                )
                jinja_dir = os.path.dirname(jinja_path)
                jinja_template = Environment(
                    loader=FileSystemLoader(jinja_dir)
                ).from_string(open(jinja_path).read())

                spark_app_id = k8s_job_name(job_id=job_id, user_id=user_id)
                pod_namespace = ConfigParams().pod_namespace

                rendered = jinja_template.render(
                    job_name=spark_app_id,
                    job_namespace=pod_namespace,
                    job_specification=job_specification_file,
                    output_dir=output_dir,
                    output_file="out",
                    log_file="stdout",
                    metadata_file=JOB_METADATA_FILENAME,
                    job_id_short=truncate_job_id_k8s(job_id),
                    job_id_full=job_id,
                    driver_cores=driver_cores,
                    driver_memory=driver_memory,
                    executor_cores=executor_cores,
                    executor_corerequest=executor_corerequest,
                    executor_memory=executor_memory,
                    executor_memory_overhead=executor_memory_overhead,
                    executor_threads_jvm=executor_threads_jvm,
                    python_max_memory = python_max,
                    max_executors=max_executors,
                    api_version=api_version,
                    dependencies="[]",  # TODO: use `serialize_dependencies()` here instead? It's probably messy to get that JSON string correctly encoded in the rendered YAML.
                    user_id=user_id,
                    max_soft_errors_ratio=max_soft_errors_ratio,
                    task_cpus=task_cpus,
                    current_time=int(time.time()),
                    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                    aws_endpoint=os.environ.get("AWS_S3_ENDPOINT","data.cloudferro.com"),
                    aws_region=os.environ.get("AWS_REGION","RegionOne"),
                    swift_url=os.environ.get("SWIFT_URL"),
                    image_name=os.environ.get("IMAGE_NAME"),
                    swift_bucket=bucket,
                    zookeeper_nodes=os.environ.get("ZOOKEEPERNODES"),
                    eodata_mount=eodata_mount,
                    datashim=os.environ.get("DATASHIM", ""),
                    archives=archives,
                    logging_threshold=logging_threshold,
                    mount_tmp=mount_tmp,
                    use_pvc=use_pvc,
                    sentinelhub_client_id_default=self._default_sentinel_hub_client_id,
                    sentinelhub_client_secret_default=self._default_sentinel_hub_client_secret
                )

                api_instance = kube_client()

                dict_ = yaml.safe_load(rendered)

                try:
                    submit_response = api_instance.create_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", pod_namespace, "sparkapplications", dict_, pretty=True)
                    logger.info(f"mapped job_id {job_id} to application ID {spark_app_id}", extra={'job_id': job_id})
                    dbl_registry.set_application_id(job_id, user_id, spark_app_id)
                    status_response = {}
                    retry=0
                    while('status' not in status_response and retry<10):
                        retry+=1
                        time.sleep(10)
                        try:
                            status_response = api_instance.get_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", pod_namespace, "sparkapplications",
                                                                                        spark_app_id)
                        except ApiException as e:
                            logger.info("Exception when calling CustomObjectsApi->list_custom_object: %s\n" % e, extra={'job_id': job_id})

                    if('status' not in status_response):
                        logger.warning(f"invalid status response: {status_response}, assuming it is queued.", extra={'job_id': job_id})
                        dbl_registry.set_status(job_id, user_id, JOB_STATUS.QUEUED)

                except ApiException as e:
                    logger.error("Exception when calling CustomObjectsApi->list_custom_object: %s\n" % e, extra={'job_id': job_id})
                    dbl_registry.set_status(job_id, user_id, JOB_STATUS.ERROR)

            else:
                # TODO: remove old submit scripts?
                submit_script = 'submit_batch_job.sh'
                if( pysparkversion.startswith('2.4')):
                    submit_script = 'submit_batch_job_spark24.sh'
                elif(sys.version_info[0]>=3 and sys.version_info[1]>=8):
                    submit_script = 'submit_batch_job_spark3.sh'
                script_location = pkg_resources.resource_filename('openeogeotrellis.deploy', submit_script)

                # TODO: use different root dir for these temp input files than self._output_root_dir (which is for output files)?
                with tempfile.NamedTemporaryFile(
                    mode="wt",
                    encoding="utf-8",
                    dir=self._output_root_dir,
                    prefix=f"{job_id}_",
                    suffix=".in",
                ) as temp_input_file, tempfile.NamedTemporaryFile(
                    mode="wt",
                    encoding="utf-8",
                    dir=self._output_root_dir,
                    prefix=f"{job_id}_",
                    suffix=".properties",
                ) as temp_properties_file:
                    temp_input_file.write(job_info["specification"])
                    temp_input_file.flush()

                    self._write_sensitive_values(temp_properties_file,
                                                 vault_token=None if sentinel_hub_client_alias == 'default'
                                                 else get_vault_token(sentinel_hub_client_alias))
                    temp_properties_file.flush()

                    # TODO: implement a saner way of passing arguments
                    job_name = "openEO batch_{title}_{j}_user {u}".format(title=job_title,j=job_id, u=user_id)
                    args = [script_location,
                            job_name,
                            temp_input_file.name,
                            str(self.get_job_output_dir(job_id)),
                            "out",  # TODO: how support multiple output files?
                            "log",
                            JOB_METADATA_FILENAME,
                            ]

                    if self._principal is not None and self._key_tab is not None:
                        args.append(self._principal)
                        args.append(self._key_tab)
                    else:
                        args.append("no_principal")
                        args.append("no_keytab")

                    args.append(job_info.get('proxy_user') or user_id)

                    if api_version:
                        args.append(api_version)
                    else:
                        args.append("0.4.0")

                    args.append(driver_memory)
                    args.append(executor_memory)
                    args.append(executor_memory_overhead)
                    args.append(driver_cores)
                    args.append(executor_cores)
                    args.append(driver_memory_overhead)
                    args.append(queue)
                    args.append(profile)
                    args.append(serialize_dependencies())
                    args.append(self.get_submit_py_files())
                    args.append(max_executors)
                    args.append(user_id)
                    args.append(job_id)
                    args.append(max_soft_errors_ratio)
                    args.append(task_cpus)
                    args.append(sentinel_hub_client_alias)
                    args.append(temp_properties_file.name)
                    args.append(archives)
                    args.append(logging_threshold)

                    persistent_worker_count = ConfigParams().persistent_worker_count
                    if persistent_worker_count != 0:
                        # Write args to persistent_worker_dir as job_<job_id>.json
                        # Also write process graph to pg_<job_id>.json
                        persistent_worker_dir = ConfigParams().persistent_worker_dir
                        if not os.path.exists(persistent_worker_dir):
                            os.makedirs(persistent_worker_dir)
                        pg_file_path = persistent_worker_dir / f"pg_{job_id}.json"
                        persistent_args = [
                            str(pg_file_path), str(self.get_job_output_dir(job_id)), "out", "log", JOB_METADATA_FILENAME,
                            args[10], serialize_dependencies(), user_id, max_soft_errors_ratio, sentinel_hub_client_alias
                        ]
                        with open(os.path.join(persistent_worker_dir, f"job_{job_id}.json"), "w") as f:
                            f.write(json.dumps(persistent_args))
                        with open(os.path.join(persistent_worker_dir, pg_file_path), "w") as f:
                            f.write(job_info["specification"])
                        # Generate our own random application id.
                        application_id = f"{random.randint(1000000000000, 9999999999999)}_{random.randint(1000000, 9999999)}"
                        output_string = f"Application report for application_{application_id} (state: running)"
                    else:
                        try:
                            logger.info(f"Submitting job with command {args!r}", extra={'job_id': job_id})
                            output_string = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
                            logger.info(f"Submitted job, output was: {output_string}", extra={'job_id': job_id})
                        except CalledProcessError as e:
                            logger.error(f"Submitting job failed, output was: {e.stdout}", exc_info=True,
                                         extra={'job_id': job_id})
                            raise e

                try:
                    application_id = self._extract_application_id(output_string)
                    logger.info("mapped job_id %s to application ID %s" % (job_id, application_id),
                                extra={'job_id': job_id})

                    dbl_registry.set_application_id(job_id, user_id, application_id)
                    dbl_registry.set_status(job_id, user_id, JOB_STATUS.QUEUED)

                except _BatchJobError as e:
                    traceback.print_exc(file=sys.stderr)
                    # TODO: why reraise as CalledProcessError?
                    raise CalledProcessError(1, str(args), output=output_string)

    def _write_sensitive_values(self, output_file, vault_token: Optional[str]):
        output_file.write(f"spark.openeo.sentinelhub.client.id.default={self._default_sentinel_hub_client_id}\n")
        output_file.write(f"spark.openeo.sentinelhub.client.secret.default={self._default_sentinel_hub_client_secret}\n")

        if vault_token is not None:
            output_file.write(f"spark.openeo.vault.token={vault_token}\n")

    @staticmethod
    def _extract_application_id(stream) -> str:
        regex = re.compile(r"^.*Application report for (application_\d{13}_\d+)\s\(state:.*", re.MULTILINE)
        match = regex.search(stream)
        if match:
            return match.group(1)
        else:
            raise _BatchJobError(stream)

    # TODO: encapsulate this SHub stuff in a dedicated class?
    def _scheduled_sentinelhub_batch_processes(
        self,
        process_graph: dict,
        api_version: Union[str, None],
        dbl_registry: DoubleJobRegistry,
        user_id: str,
        job_id: str,
        job_options: dict,
        sentinel_hub_client_alias: str,
        get_vault_token: Callable[[str], str],
    ) -> bool:
        # TODO: reduce code duplication between this and ProcessGraphDeserializer
        from openeo_driver.dry_run import DryRunDataTracer
        from openeo_driver.ProcessGraphDeserializer import convert_node, ENV_DRY_RUN_TRACER
        from openeo.internal.process_graph_visitor import ProcessGraphVisitor

        env = EvalEnv(
            {
                "user": User(user_id),
                # TODO #285: use original GeoPySparkBackendImplementation instead of recreating a new one
                #           or at least set all GeoPySparkBackendImplementation arguments correctly (e.g. through a config)
                "backend_implementation": GeoPySparkBackendImplementation(
                    use_job_registry=False,
                ),
            }
        )

        if api_version:
            env = env.push({"version": api_version})

        top_level_node = ProcessGraphVisitor.dereference_from_node_arguments(process_graph)
        result_node = process_graph[top_level_node]

        dry_run_tracer = DryRunDataTracer()
        convert_node(result_node, env=env.push({ENV_DRY_RUN_TRACER: dry_run_tracer, ENV_SAVE_RESULT:[]}))

        source_constraints = dry_run_tracer.get_source_constraints()
        logger.info("Dry run extracted these source constraints: {s}".format(s=source_constraints),
                    extra={'job_id': job_id})

        batch_process_dependencies = []
        batch_request_cache = {}

        for (process, arguments), constraints in source_constraints:
            if process == 'load_collection':
                collection_id, properties_criteria = arguments

                band_names = constraints.get('bands')

                metadata = GeopysparkCubeMetadata(self._catalog.get_collection_metadata(collection_id))
                if band_names:
                    metadata = metadata.filter_bands(band_names)

                layer_source_info = metadata.get("_vito", "data_source")
                sar_backscatter_compatible = layer_source_info.get("sar_backscatter_compatible", False)

                if "sar_backscatter" in constraints and not sar_backscatter_compatible:
                    raise OpenEOApiException(message=
                                             """Process "sar_backscatter" is not applicable for collection {c}."""
                                             .format(c=collection_id), status_code=400)

                if layer_source_info['type'] == 'sentinel-hub':
                    sar_backscatter_arguments: Optional[SarBackscatterArgs] = (
                        constraints.get("sar_backscatter", SarBackscatterArgs()) if sar_backscatter_compatible
                        else None
                    )

                    card4l = (sar_backscatter_arguments is not None
                              and sar_backscatter_arguments.coefficient == "gamma0-terrain"
                              and sar_backscatter_arguments.mask
                              and sar_backscatter_arguments.local_incidence_angle)

                    spatial_extent = constraints['spatial_extent']
                    crs = spatial_extent['crs']

                    def get_geometries():
                        return (constraints.get("aggregate_spatial", {}).get("geometries") or
                                constraints.get("filter_spatial", {}).get("geometries"))

                    def area() -> float:
                        def bbox_area() -> float:
                            geom = Polygon.from_bounds(
                                xmin=spatial_extent['west'],
                                ymin=spatial_extent['south'],
                                xmax=spatial_extent['east'],
                                ymax=spatial_extent['north'])

                            return area_in_square_meters(geom, crs)

                        geometries = get_geometries()

                        if not geometries:
                            return bbox_area()
                        elif isinstance(geometries, DelayedVector):
                            # TODO: can this case and the next be replaced with a combination of to_projected_polygons
                            #  and ProjectedPolygons#areaInSquareMeters?
                            return (self._jvm
                                    .org.openeo.geotrellis.ProjectedPolygons.fromVectorFile(geometries.path)
                                    .areaInSquareMeters())
                        elif isinstance(geometries, DriverVectorCube):
                            return geometries.get_area()
                        elif isinstance(geometries, shapely.geometry.base.BaseGeometry):
                            return area_in_square_meters(geometries, crs)
                        else:
                            logger.error(f"GpsBatchJobs._scheduled_sentinelhub_batch_processes:area Unhandled geometry type {type(geometries)}")
                            raise ValueError(geometries)

                    actual_area = area()
                    absolute_maximum_area = 1e+12  # 1 million km²

                    if actual_area > absolute_maximum_area:
                        raise OpenEOApiException(message=
                                                 "Requested area {a} m² for collection {c} exceeds maximum of {m} m²."
                                                 .format(a=actual_area, c=collection_id, m=absolute_maximum_area),
                                                 status_code=400)

                    def large_area() -> bool:
                        batch_process_threshold_area = 50 * 1000 * 50 * 1000  # 50x50 km²
                        large_enough = actual_area >= batch_process_threshold_area

                        logger.info("deemed collection {c} AOI ({a} m²) {s} for batch processing (threshold {t} m²)"
                                    .format(c=collection_id, a=actual_area,
                                            s="large enough" if large_enough else "too small",
                                            t=batch_process_threshold_area), extra={'job_id': job_id})

                        return large_enough

                    endpoint = layer_source_info['endpoint']
                    supports_batch_processes = (endpoint.startswith("https://services.sentinel-hub.com") or
                                                endpoint.startswith("https://services-uswest2.sentinel-hub.com"))

                    shub_input_approach = deep_get(job_options, 'sentinel-hub', 'input', default=None)

                    if not supports_batch_processes:  # always sync approach
                        logger.info("endpoint {e} does not support batch processing".format(e=endpoint),
                                    extra={'job_id': job_id})
                        continue
                    elif card4l:  # always batch approach
                        logger.info("deemed collection {c} request CARD4L compliant ({s})"
                                    .format(c=collection_id, s=sar_backscatter_arguments), extra={'job_id': job_id})
                    elif shub_input_approach == 'sync':
                        logger.info("forcing sync input processing for collection {c}".format(c=collection_id),
                                    extra={'job_id': job_id})
                        continue
                    elif shub_input_approach == 'batch':
                        logger.info("forcing batch input processing for collection {c}".format(c=collection_id),
                                    extra={'job_id': job_id})
                    elif not large_area():  # 'auto'
                        continue  # skip SHub batch process and use sync approach instead

                    sample_type = self._jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
                        layer_source_info.get('sample_type', 'UINT16'))

                    from_date, to_date = normalize_temporal_extent(constraints['temporal_extent'])

                    west = spatial_extent['west']
                    south = spatial_extent['south']
                    east = spatial_extent['east']
                    north = spatial_extent['north']
                    bbox = self._jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))

                    bucket_name = layer_source_info.get('bucket', sentinel_hub.OG_BATCH_RESULTS_BUCKET)

                    logger.debug(f"Sentinel Hub client alias: {sentinel_hub_client_alias}", extra={'job_id': job_id})

                    if sentinel_hub_client_alias == 'default':
                        sentinel_hub_client_id = self._default_sentinel_hub_client_id
                        sentinel_hub_client_secret = self._default_sentinel_hub_client_secret
                    else:
                        sentinel_hub_client_id, sentinel_hub_client_secret = (
                            self._vault.get_sentinel_hub_credentials(sentinel_hub_client_alias,
                                                                     get_vault_token(sentinel_hub_client_alias)))

                    batch_processing_service = (
                        SentinelHubBatchProcessing.get_batch_processing_service(
                            endpoint=endpoint,
                            bucket_name=bucket_name,
                            sentinel_hub_client_id=sentinel_hub_client_id,
                            sentinel_hub_client_secret=sentinel_hub_client_secret,
                            sentinel_hub_client_alias=sentinel_hub_client_alias,
                            jvm=self._jvm,
                        )
                    )

                    shub_band_names = metadata.band_names

                    if sar_backscatter_arguments and sar_backscatter_arguments.mask:
                        shub_band_names.append('dataMask')

                    if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
                        shub_band_names.append('localIncidenceAngle')

                    def metadata_properties_from_criteria() -> Dict[str, Dict[str, object]]:
                        def as_dicts(criteria):
                            return {criterion[0]: criterion[1] for criterion in criteria}  # (operator -> value)

                        metadata_properties_return = {property_name: as_dicts(criteria) for property_name, criteria in properties_criteria}
                        sentinel_hub.assure_polarization_from_sentinel_bands(shub_band_names,
                                                                             metadata_properties_return, job_id)
                        return metadata_properties_return

                    metadata_properties = metadata_properties_from_criteria()

                    geometries = get_geometries()

                    if not geometries:
                        geometry = bbox
                        # string crs is unchanged
                    else:
                        projected_polygons = to_projected_polygons(self._jvm, geometry=geometries, crs=crs, buffer_points=True)
                        geometry = projected_polygons.polygons()
                        crs = projected_polygons.crs()

                    class BadlyHashable:
                        """
                        Simplifies implementation by allowing unhashable types in a dict-based cache. The number of
                        items in this cache is very small anyway.
                        """

                        def __init__(self, target):
                            self.target = target

                        def __eq__(self, other):
                            return isinstance(other, BadlyHashable) and self.target == other.target

                        def __hash__(self):
                            return 0

                        def __repr__(self):
                            return f"BadlyHashable({repr(self.target)})"

                    if not geometries:
                        hashable_geometry = (bbox.xmin(), bbox.ymin(), bbox.xmax(), bbox.ymax())
                    elif isinstance(geometries, DelayedVector):
                        hashable_geometry = geometries.path
                    else:
                        hashable_geometry = BadlyHashable(geometries)

                    collecting_folder: Optional[str] = None

                    if card4l:
                        # TODO: not obvious but this does the validation as well
                        dem_instance = sentinel_hub.processing_options(collection_id, sar_backscatter_arguments)\
                            .get('demInstance')

                        # these correspond to the .start_card4l_batch_processes arguments
                        batch_request_cache_key = (
                            collection_id,  # for 'collection_id' and 'dataset_id'
                            hashable_geometry,
                            str(crs),
                            from_date,
                            to_date,
                            to_hashable(shub_band_names),
                            dem_instance,
                            to_hashable(metadata_properties)
                        )

                        batch_request_ids, subfolder = batch_request_cache.get(batch_request_cache_key, (None, None))

                        if batch_request_ids is None:
                            # cannot be the batch job ID because results for multiple collections would end up in
                            #  the same S3 dir
                            request_group_uuid = str(uuid.uuid4())
                            subfolder = request_group_uuid

                            # return type py4j.java_collections.JavaList is not JSON serializable
                            batch_request_ids = list(batch_processing_service.start_card4l_batch_processes(
                                layer_source_info['collection_id'],
                                layer_source_info['dataset_id'],
                                geometry,
                                crs,
                                from_date,
                                to_date,
                                shub_band_names,
                                dem_instance,
                                metadata_properties,
                                subfolder,
                                request_group_uuid)
                            )

                            batch_request_cache[batch_request_cache_key] = (batch_request_ids, subfolder)

                            logger.info("saved newly scheduled CARD4L batch processes {b} for near future use"
                                        " (key {k!r})".format(b=batch_request_ids, k=batch_request_cache_key),
                                        extra={'job_id': job_id})
                        else:
                            logger.debug("recycling saved CARD4L batch processes {b} (key {k!r})".format(
                                b=batch_request_ids, k=batch_request_cache_key), extra={'job_id': job_id})
                    else:
                        shub_caching_flag = deep_get(job_options, 'sentinel-hub', 'cache-results', default=None)
                        try_cache = (ConfigParams().cache_shub_batch_results if shub_caching_flag is None  # auto
                                     else shub_caching_flag)
                        can_cache = layer_source_info.get('cacheable', False)
                        cache = try_cache and can_cache

                        processing_options = (sentinel_hub.processing_options(collection_id, sar_backscatter_arguments)
                                              if sar_backscatter_arguments else {})

                        # these correspond to the .start_batch_process/start_batch_process_cached arguments
                        batch_request_cache_key = (
                            collection_id,  # for 'collection_id', 'dataset_id' and sample_type
                            hashable_geometry,
                            str(crs),
                            from_date,
                            to_date,
                            to_hashable(shub_band_names),
                            to_hashable(metadata_properties),
                            to_hashable(processing_options)
                        )

                        if cache:
                            (batch_request_id, subfolder,
                             collecting_folder) = batch_request_cache.get(batch_request_cache_key, (None, None, None))

                            if collecting_folder is None:
                                subfolder = generate_unique_id()  # batch process context JSON is written here as well

                                # collecting_folder must be writable from driver (cached tiles) and async_task
                                # handler (new tiles))
                                collecting_folder = f"/tmp_epod/openeo_collecting/{subfolder}"
                                os.mkdir(collecting_folder)
                                os.chmod(collecting_folder, mode=0o770)  # umask prevents group write

                                batch_request_id = batch_processing_service.start_batch_process_cached(
                                    layer_source_info['collection_id'],
                                    layer_source_info['dataset_id'],
                                    geometry,
                                    crs,
                                    from_date,
                                    to_date,
                                    shub_band_names,
                                    sample_type,
                                    metadata_properties,
                                    processing_options,
                                    subfolder,
                                    collecting_folder
                                )

                                batch_request_cache[batch_request_cache_key] = (batch_request_id, subfolder,
                                                                                collecting_folder)

                                logger.info("saved newly scheduled cached batch process {b} for near future use"
                                            " (key {k!r})".format(b=batch_request_id, k=batch_request_cache_key),
                                            extra={'job_id': job_id})
                            else:
                                logger.debug("recycling saved cached batch process {b} (key {k!r})".format(
                                    b=batch_request_id, k=batch_request_cache_key), extra={'job_id': job_id})

                            batch_request_ids = [batch_request_id]
                        else:
                            try:
                                batch_request_id = batch_request_cache.get(batch_request_cache_key)

                                if batch_request_id is None:
                                    batch_request_id = batch_processing_service.start_batch_process(
                                        layer_source_info['collection_id'],
                                        layer_source_info['dataset_id'],
                                        geometry,
                                        crs,
                                        from_date,
                                        to_date,
                                        shub_band_names,
                                        sample_type,
                                        metadata_properties,
                                        processing_options
                                    )

                                    batch_request_cache[batch_request_cache_key] = batch_request_id

                                    logger.info("saved newly scheduled batch process {b} for near future use"
                                                " (key {k!r})".format(b=batch_request_id, k=batch_request_cache_key),
                                                extra={'job_id': job_id})
                                else:
                                    logger.debug("recycling saved batch process {b} (key {k!r})".format(
                                        b=batch_request_id, k=batch_request_cache_key), extra={'job_id': job_id})

                                subfolder = batch_request_id
                                batch_request_ids = [batch_request_id]
                            except Py4JJavaError as e:
                                java_exception = e.java_exception

                                if (java_exception.getClass().getName() ==
                                        'org.openeo.geotrellissentinelhub.BatchProcessingService$NoSuchFeaturesException'):
                                    raise OpenEOApiException(
                                        message=f"{java_exception.getClass().getName()}: {java_exception.getMessage()}",
                                        status_code=400)
                                else:
                                    raise e

                    batch_process_dependencies.append(dict_no_none(
                        collection_id=collection_id,
                        batch_request_ids=batch_request_ids,  # to poll SHub
                        collecting_folder=collecting_folder,  # temporary cached and new single band tiles, also a flag
                        results_location=f"s3://{bucket_name}/{subfolder}",  # new multiband tiles
                        card4l=card4l  # should the batch job expect CARD4L metadata?
                    ))

        if batch_process_dependencies:
            dbl_registry.set_dependencies(
                job_id=job_id, user_id=user_id, dependencies=batch_process_dependencies
            )
            return True

        return False

    def get_result_assets(self, job_id: str, user_id: str) -> Dict[str, dict]:
        """
        Reads the metadata json file from the job directory
        and returns information about the output files.

        :param job_id: The id of the job to get the results for.
        :param user_id: The id of the user that started the job.

        :return: A mapping between a filename and a dict containing information about that file.
        """
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status != JOB_STATUS.FINISHED:
            raise JobNotFinishedException

        job_dir = self.get_job_output_dir(job_id=job_id)

        results_metadata = self.get_results_metadata(job_id, user_id)
        out_assets = results_metadata.get("assets", {})
        out_metadata = out_assets.get("out", {})
        bands = [Band(*properties) for properties in out_metadata.get("bands", [])]
        nodata = out_metadata.get("nodata", None)
        media_type = out_metadata.get("type", out_metadata.get("media_type", "application/octet-stream"))

        results_dict = {}

        # ml_model_metadata is added to the metadata when a batch job is completed.
        ml_model_metadata = results_metadata.get("ml_model_metadata", None)
        if ml_model_metadata is not None:
            ml_model_metadata['ml_model_metadata'] = True
            ml_model_metadata['asset'] = False
            results_dict['ml_model_metadata.json'] = ml_model_metadata

        # If we are retrieving the result files from object storage (S3) then the job directory
        # might not exist inside this container.
        # Normally we would use object storage with a Kubernetes deployment and the original
        # container that ran the job can already be gone.
        # We only want to apply the cases below when we effectively have a job directory:
        # it should exists and should be a directory.
        if job_dir.is_dir():
            if os.path.isfile(job_dir / 'out'):
                results_dict['out'] = {
                    # TODO: give meaningful filename and extension
                    "asset": True,
                    "output_dir": str(job_dir),
                    "type": media_type,
                    "bands": bands,
                    "nodata": nodata
                }

            if os.path.isfile(job_dir / 'profile_dumps.tar.gz'):
                results_dict['profile_dumps.tar.gz'] = {
                    "asset": True,
                    "output_dir": str(job_dir),
                    "type": "application/gzip"
                }

            for file_name in os.listdir(job_dir):
                if file_name.endswith("_metadata.json") and file_name != JOB_METADATA_FILENAME:
                    results_dict[file_name] = {
                        "asset": True,
                        "output_dir": str(job_dir),
                        "type": "application/json"
                    }
                elif file_name.endswith("_MULTIBAND.tif"):
                    results_dict[file_name] = {
                        "asset": True,
                        "output_dir": str(job_dir),
                        "type": "image/tiff; application=geotiff"
                    }

        #TODO: this is a more generic approach, we should be able to avoid and reduce the guesswork being done above
        #Batch jobs should construct the full metadata, which can be passed on, and only augmented if needed
        for title, asset in out_assets.items():
            if title not in results_dict:
                asset['asset'] = True

                # When we have retrieved the job metadata from S3 then output_dir will already be filled in,
                # and the value will point to the S3 storage. In that case we don't want to overwrite it here.
                if not ConfigParams().use_object_storage:
                    asset["output_dir"] = str(job_dir)

                if "bands" in asset:
                    asset["bands"] = [Band(**b) for b in asset["bands"]]
                results_dict[title] = asset

        return results_dict

    def get_results_metadata_path(self, job_id: str) -> Path:
        return self.get_job_output_dir(job_id) / JOB_METADATA_FILENAME

    def get_results_metadata(self, job_id: str, user_id: str) -> dict:
        """
        Reads the metadata json file from the job directory and returns it.
        """
        metadata_file = self.get_results_metadata_path(job_id=job_id)

        if ConfigParams().use_object_storage:
            try:
                contents = get_s3_file_contents(str(metadata_file))
                return json.loads(contents)
            except Exception:
                logger.warning(
                    "Could not retrieve result metadata from object storage %s",
                    metadata_file, exc_info=True,
                    extra={'job_id': job_id})

        try:
            with open(metadata_file) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Could not derive result metadata from %s", metadata_file, exc_info=True,
                           extra={'job_id': job_id})

        return {}

    def get_log_entries(
        self,
        job_id: str,
        user_id: str,
        offset: Optional[str] = None,
        level: Optional[str] = None,
    ) -> Iterable[dict]:
        # will throw if job doesn't match user
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status in [JOB_STATUS.CREATED, JOB_STATUS.QUEUED]:
            return iter(())

        return elasticsearch_logs(
            job_id=job_id, create_time=job_info.created, offset=offset, level=level
        )

    def cancel_job(self, job_id: str, user_id: str):
        with self._double_job_registry as registry:
            job_info = registry.get_job(job_id, user_id)

        if job_info["status"] in [
            JOB_STATUS.CREATED,
            JOB_STATUS.FINISHED,
            JOB_STATUS.ERROR,
            JOB_STATUS.CANCELED,
        ]:
            return

        application_id = job_info['application_id']
        logger.debug(f"Cancelling job with application_id: {application_id}")

        if application_id:  # can be empty if awaiting SHub dependencies (OpenEO status 'queued')
            if ConfigParams().is_kube_deploy:
                api_instance = kube_client()
                group = "sparkoperator.k8s.io"
                version = "v1beta2"
                namespace = ConfigParams().pod_namespace
                plural = "sparkapplications"
                name = application_id
                logger.debug(f"Sending API call to kubernetes to delete job: {name}")
                delete_response = api_instance.delete_namespaced_custom_object(group, version, namespace, plural, name)
                logger.debug(
                    f"Killed corresponding Spark job {application_id} with kubernetes API call "
                    f"DELETE /apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}",
                    extra = {'job_id': job_id, 'API response': delete_response}
                )
                with self._double_job_registry:
                    registry.set_status(job_id, user_id, JOB_STATUS.CANCELED)
            else:
                try:
                    kill_spark_job = subprocess.run(
                        ["curl", "--location-trusted", "--fail", "--negotiate", "-u", ":", "--insecure", "-X", "PUT",
                         "-H", "Content-Type: application/json", "-d", '{"state": "KILLED"}',
                         f"https://epod-master1.vgt.vito.be:8090/ws/v1/cluster/apps/{application_id}/state"],
                        timeout=20,
                        check=True,
                        universal_newlines=True,
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT  # combine both output streams into one
                    )

                    logger.debug(f"Killed corresponding Spark job {application_id} with command {kill_spark_job.args!r}",
                                 extra={'job_id': job_id})
                except CalledProcessError as e:
                    logger.warning(f"Could not kill corresponding Spark job {application_id}, output was: {e.stdout}",
                                   exc_info=True, extra={'job_id': job_id})
                finally:
                    with self._double_job_registry as registry:
                        registry.set_status(job_id, user_id, JOB_STATUS.CANCELED)
        else:
            with self._double_job_registry as registry:
                registry.remove_dependencies(job_id, user_id)
                registry.set_status(job_id, user_id, JOB_STATUS.CANCELED)

    def delete_job(self, job_id: str, user_id: str):
        self._delete_job(job_id, user_id, propagate_errors=False)

    def _delete_job(self, job_id: str, user_id: str, propagate_errors: bool):
        try:
            self.cancel_job(job_id, user_id)
        except InternalException:  # job never started, not an error
            pass
        except CalledProcessError as e:
            if e.returncode == 255 and "doesn't exist in RM" in e.stdout:  # already finished and gone, not an error
                pass
            elif propagate_errors:
                raise
            else:
                logger.warning("Unable to kill corresponding Spark job for job {j}: {a!r}\n{o}".format(j=job_id, a=e.cmd,
                                                                                                       o=e.stdout),
                               exc_info=e, extra={'job_id': job_id})

        job_dir = self.get_job_output_dir(job_id)

        try:
            shutil.rmtree(job_dir)
        except FileNotFoundError:  # nothing to delete, not an error
            pass
        except Exception as e:
            # always log because the exception doesn't show the job directory
            logger.warning("Could not recursively delete {p}".format(p=job_dir), exc_info=e, extra={'job_id': job_id})
            if propagate_errors:
                raise

        with self._double_job_registry as registry:
            job_info = registry.get_job(job_id, user_id)
        dependency_sources = ZkJobRegistry.get_dependency_sources(job_info)

        if dependency_sources:
            # Only for SentinelHub batch processes.
            self.delete_batch_process_dependency_sources(job_id, dependency_sources, propagate_errors)

        config_params = ConfigParams()
        if config_params.is_kube_deploy:
            # Kubernetes batch jobs are stored using s3 object storage.
            logger.info(f"Kube_deploy: Deleting directory from s3 object storage.")
            prefix = str(self.get_job_output_dir(job_id))[1:]
            self._jvm.org.openeo.geotrellis.creo.CreoS3Utils.deleteCreoSubFolder(
                config_params.s3_bucket_name, prefix
            )

        with self._double_job_registry as registry:
            registry.delete(job_id, user_id)

        logger.info("Deleted job {u}/{j}".format(u=user_id, j=job_id), extra={'job_id': job_id})

    @deprecated("call delete_batch_process_dependency_sources instead")
    def delete_batch_process_results(self, job_id: str, subfolders: List[str], propagate_errors: bool):
        dependency_sources = [f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}/{subfolder}" for subfolder in subfolders]
        self.delete_batch_process_dependency_sources(job_id, dependency_sources, propagate_errors)

    def delete_batch_process_dependency_sources(self, job_id: str, dependency_sources: List[str],
                                                propagate_errors: bool):
        def is_disk_source(dependency_source: str) -> bool:
            return dependency_source.startswith("file:")

        def is_s3_source(dependency_source: str) -> bool:
            return not is_disk_source(dependency_source)

        results_locations = [source for source in dependency_sources if is_s3_source(source)]
        assembled_folders = [urlparse(source).path for source in dependency_sources if is_disk_source(source)]

        s3_service = self._jvm.org.openeo.geotrellissentinelhub.S3Service()

        for results_location in results_locations:
            uri_parts = urlparse(results_location)
            bucket_name = uri_parts.hostname
            subfolder = uri_parts.path[1:]

            try:
                s3_service.delete_batch_process_results(bucket_name, subfolder)
            except Py4JJavaError as e:
                java_exception = e.java_exception

                if (java_exception.getClass().getName() ==
                        'org.openeo.geotrellissentinelhub.S3Service$UnknownFolderException'):
                    logger.warning("Could not delete unknown Sentinel Hub result folder s3://{b}/{f}"
                                   .format(b=bucket_name, f=subfolder), extra={'job_id': job_id})
                elif propagate_errors:
                    raise
                else:
                    logger.warning("Could not delete Sentinel Hub result folder s3://{b}/{f}"
                                   .format(b=bucket_name, f=subfolder), exc_info=e, extra={'job_id': job_id})

        if results_locations:
            logger.info("Deleted Sentinel Hub result folder(s) {fs} for batch job {j}".format(fs=results_locations,
                                                                                              j=job_id),
                        extra={'job_id': job_id})

        for assembled_folder in assembled_folders:
            try:
                shutil.rmtree(assembled_folder)
            except Exception as e:
                logger.warning("Could not recursively delete {p}".format(p=assembled_folder), exc_info=e,
                               extra={'job_id': job_id})
                if propagate_errors:
                    raise

        if assembled_folders:
            logger.info("Deleted Sentinel Hub assembled folder(s) {fs} for batch job {j}"
                        .format(fs=assembled_folders, j=job_id), extra={'job_id': job_id})

    def delete_jobs_before(
        self,
        upper: datetime,
        *,
        user_ids: Optional[List[str]] = None,
        dry_run: bool = True,
        include_ongoing: bool = True,
        include_done: bool = True,
        user_limit: Optional[int] = 1000,
    ) -> None:
        with self._double_job_registry as registry, TimingLogger(
            title=f"Collecting jobs to delete: {upper=} {user_ids=} {include_ongoing=} {include_done=}",
            logger=logger,
        ):
            jobs_before = registry.get_all_jobs_before(
                upper,
                user_ids=user_ids,
                include_ongoing=include_ongoing,
                include_done=include_done,
                user_limit=user_limit,
            )
        logger.info(f"Collected {len(jobs_before)} jobs to delete")

        with TimingLogger(title=f"Deleting {len(jobs_before)} jobs", logger=logger):

            for job_info in jobs_before:
                job_id = job_info["job_id"]
                user_id = job_info["user_id"]
                if not dry_run:
                    logger.info(f"Deleting {job_id=} from {user_id=}")
                    self._delete_job(
                        job_id=job_id, user_id=user_id, propagate_errors=True
                    )
                else:
                    logger.info(f"Dry run: not deleting {job_id=} from {user_id=}")



class _BatchJobError(Exception):
    pass
