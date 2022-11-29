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
from pystac import Collection
from shapely.geometry import box, Polygon

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.metadata import TemporalDimension, SpatialDimension, Band, BandDimension
from openeo.util import dict_no_none, rfc3339, deep_get
from openeo_driver import backend
from openeo_driver.ProcessGraphDeserializer import ConcreteProcessing
from openeo_driver.backend import (ServiceMetadata, BatchJobMetadata, OidcProvider, ErrorSummary, LoadParameters,
                                   CollectionCatalog)
from openeo_driver.datacube import DriverVectorCube
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.dry_run import SourceConstraint
from openeo_driver.errors import (JobNotFinishedException, OpenEOApiException, InternalException,
                                  ServiceUnsupportedException)
from openeo_driver.save_result import ImageCollectionResult
from openeo_driver.users import User
from openeo_driver.util.logging import FlaskRequestCorrelationIdLogging, FlaskUserIdLogging
from openeo_driver.util.utm import area_in_square_meters, auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv, to_hashable, generate_unique_id
from openeogeotrellis import sentinel_hub
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.layercatalog import get_layer_catalog, check_missing_products
from openeogeotrellis.logs import elasticsearch_logs
from openeogeotrellis.ml.GeopySparkCatBoostModel import CatBoostClassificationModel
from openeogeotrellis.service_registry import (InMemoryServiceRegistry, ZooKeeperServiceRegistry,
                                               AbstractServiceRegistry, SecondaryService, ServiceEntity)
from openeogeotrellis.traefik import Traefik
from openeogeotrellis.user_defined_process_repository import ZooKeeperUserDefinedProcessRepository, \
    InMemoryUserDefinedProcessRepository
from openeogeotrellis.utils import kerberos, zk_client, to_projected_polygons, normalize_temporal_extent, \
    truncate_job_id_k8s, dict_merge_recursive, single_value, add_permissions, get_jvm, mdc_include, mdc_remove
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

    def __init__(self, use_zookeeper=True, opensearch_enrich=True):
        # TODO: do this with a config instead of hardcoding rules?
        self._service_registry = (
            InMemoryServiceRegistry() if not use_zookeeper or ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )

        user_defined_processes = (
            # choosing between DBs can be done in said config
            InMemoryUserDefinedProcessRepository() if not use_zookeeper or ConfigParams().is_ci_context
            else ZooKeeperUserDefinedProcessRepository(hosts=ConfigParams().zookeepernodes)
        )

        vault = Vault("https://vault.vgt.vito.be")

        catalog = get_layer_catalog(vault, opensearch_enrich=opensearch_enrich)

        jvm = get_jvm()

        conf = SparkContext.getOrCreate().getConf()
        principal = conf.get("spark.yarn.principal", conf.get("spark.kerberos.principal"))
        key_tab = conf.get("spark.yarn.keytab", conf.get("spark.kerberos.keytab"))

        super().__init__(
            catalog=catalog,
            batch_jobs=GpsBatchJobs(catalog, jvm, principal, key_tab, vault),
            user_defined_processes=user_defined_processes,
            processing=GpsProcessing(),
            # secondary_services=GpsSecondaryServices(service_registry=self._service_registry),
        )

        self._principal = principal
        self._key_tab = key_tab

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
        # TODO Move these providers to config or bootstrap script?
        default_client_egi = {
            "id": "vito-default-client",
            "grant_types": [
                "authorization_code+pkce",
                "urn:ietf:params:oauth:grant-type:device_code+pkce",
                "refresh_token",
            ],
            "redirect_urls": [
                "https://editor.openeo.org",
                "http://localhost:1410/",
            ]
        }
        return [
            OidcProvider(
                id="egi",
                issuer="https://aai.egi.eu/auth/realms/egi/",
                scopes=[
                    "openid", "email",
                    "eduperson_entitlement",
                    "eduperson_scoped_affiliation",
                ],
                title="EGI Check-in",
                default_clients=[default_client_egi],
            ),
            # TODO: remove old EGI provider (issuer https://aai.egi.eu/oidc/)
            OidcProvider(
                id="egi-legacy",
                issuer="https://aai.egi.eu/oidc/",
                scopes=[
                    "openid", "email",
                    "eduperson_entitlement",
                    "eduperson_scoped_affiliation",
                ],
                title="EGI Check-in (legacy)",
                default_clients=[default_client_egi],
            ),
            # TODO: provide only one EGI Check-in variation? Or only include EGI Check-in dev instance on openeo-dev?
            OidcProvider(
                id="egi-dev",
                issuer="https://aai-dev.egi.eu/auth/realms/egi",
                scopes=[
                    "openid", "email",
                    "eduperson_entitlement",
                    "eduperson_scoped_affiliation",
                ],
                title="EGI Check-in (dev)",
                default_clients=[default_client_egi],
            ),
            OidcProvider(
                id="keycloak",
                issuer="https://sso.vgt.vito.be/auth/realms/terrascope",
                scopes=["openid", "email"],
                title="VITO Keycloak",
            ),
        ]

    def file_formats(self) -> dict:
        return {
            "input": {
                "GeoJSON": {
                    "gis_data_types": ["vector"],
                    "parameters": {},
                },
                "Parquet": {
                    "gis_data_types": ["vector"],
                    "parameters": {},
                }
            },
            "output": {
                "GTiff": {
                    "title": "GeoTiff",
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
                    "gis_data_types": ["other"],  # TODO: also "raster", "vector", "table"?
                    "parameters": {},
                },
                "netCDF": {
                    "title": "Network Common Data Form",
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
                    "gis_data_types": ["raster"],
                    "parameters": {},
                },
                "CSV": {
                    "title": "Comma Separated Values",
                    "gis_data_types": ["raster"],
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

        spatial_extent = load_params.spatial_extent
        west = spatial_extent.get("west", None)
        east = spatial_extent.get("east", None)
        north = spatial_extent.get("north", None)
        south = spatial_extent.get("south", None)
        crs = spatial_extent.get("crs", None)
        spatial_bounds_present = all(b is not None for b in [west, south, east, north])

        if job_id.startswith("http://") or job_id.startswith("https://"):
            job_results_canonical_url = job_id
            job_results = Collection.from_file(href=job_results_canonical_url)

            def reproject(bbox: List[float], crs_from, crs_to) -> List[float]:
                from pyproj import Transformer
                transform = Transformer.from_crs(crs_from, crs_to, always_xy=True).transform
                xmin, ymin = transform(xx=bbox[0], yy=bbox[1])
                xmax, ymax = transform(xx=bbox[2], yy=bbox[3])
                return [xmin, ymin, xmax, ymax]

            def intersects_spatial_extent(item) -> bool:
                if not spatial_bounds_present or item.bbox is None:
                    return True

                spatial_extent_epsg4326 = reproject([west, south, east, north],
                                                    crs_from=crs or "EPSG:4326",
                                                    crs_to="EPSG:4326")

                return Polygon.from_bounds(*spatial_extent_epsg4326).intersects(Polygon.from_bounds(*item.bbox))

            uris_with_metadata = {asset.get_absolute_href(): (item.datetime.isoformat(),
                                                              asset.extra_fields.get("eo:bands", []))
                                  for item in job_results.get_items()
                                  if intersects_spatial_extent(item)
                                  for asset in item.get_assets().values()
                                  if asset.media_type == "image/tiff; application=geotiff"}

            timestamped_uris = {uri: timestamp for uri, (timestamp, _) in uris_with_metadata.items()}

            try:
                eo_bands = single_value(eo_bands for _, eo_bands in uris_with_metadata.values())
                band_names = [eo_band["name"] for eo_band in eo_bands]
            except ValueError as e:
                raise OpenEOApiException(message=f"Unsupported band information for job {job_id}: {str(e)}",
                                         status_code=501)

            def load_spatial_bounds_from_job_results():
                overall_spatial_extent = job_results.extent.spatial.bboxes[0]
                best_epsg = auto_utm_epsg_for_geometry(box(*overall_spatial_extent))
                return reproject(overall_spatial_extent, 4326, best_epsg), best_epsg

            load_spatial_bounds = load_spatial_bounds_from_job_results

        else:
            paths_with_metadata = {asset["href"]: (asset.get("datetime"), asset.get("bands", []))
                                   for _, asset in self.batch_jobs.get_results(job_id=job_id, user_id=user_id).items()
                                   if asset["type"] == "image/tiff; application=geotiff"}

            if len(paths_with_metadata) == 0:
                raise OpenEOApiException(message=f"Job {job_id} contains no results of supported type GTiff.",
                                         status_code=501)

            if not all(timestamp is not None for timestamp, _ in paths_with_metadata.values()):
                raise OpenEOApiException(
                    message=f"Cannot load results of job {job_id} because they lack timestamp information.",
                    status_code=400)

            timestamped_uris = {path: timestamp for path, (timestamp, _) in paths_with_metadata.items()}

            try:
                eo_bands = single_value(eo_bands for _, eo_bands in paths_with_metadata.values())
                band_names = [eo_band.name for eo_band in eo_bands]
            except ValueError as e:
                raise OpenEOApiException(message=f"Unsupported band information for job {job_id}: {str(e)}",
                                         status_code=501)

            def load_spatial_bounds_from_job_info():
                job_info = self.batch_jobs.get_job_info(job_id, user_id)
                return [job_info.bbox[0], job_info.bbox[1], job_info.bbox[2], job_info.bbox[3]], job_info.epsg

            load_spatial_bounds = load_spatial_bounds_from_job_info

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

        bands = load_params.bands
        if bands:
            band_indices = [metadata.get_band_index(b) for b in bands]
            metadata = metadata.filter_bands(bands)
        else:
            band_indices = None

        jvm = get_jvm()

        pyramid_factory = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_uris(timestamped_uris)

        single_level = env.get('pyramid_levels', 'all') != 'all'

        if single_level:
            existing_bbox, existing_epsg = load_spatial_bounds()

            if spatial_bounds_present:
                extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))
            else:
                extent = jvm.geotrellis.vector.Extent(*[float(value) for value in existing_bbox])
                crs = "EPSG:4326"

            projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, crs)
            projected_polygons = (getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")
                                  .reproject(projected_polygons, existing_epsg))

            metadata_properties = None
            correlation_id = None
            data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
            getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")

            pyramid = pyramid_factory.datacube_seq(projected_polygons, from_date, to_date, metadata_properties,
                                                   correlation_id, data_cube_parameters)
        else:
            extent = (jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))
                      if spatial_bounds_present else None)

            pyramid = pyramid_factory.pyramid_seq(extent, crs, from_date, to_date)

        metadata = metadata.filter_bbox(west=extent.xmin(), south=extent.ymin(), east=extent.xmax(),
                                        north=extent.ymax(), crs=crs)

        temporal_tiled_raster_layer = jvm.geopyspark.geotrellis.TemporalTiledRasterLayer
        option = jvm.scala.Option

        # noinspection PyProtectedMember
        levels = {pyramid.apply(index)._1(): TiledRasterLayer(LayerType.SPACETIME, temporal_tiled_raster_layer(
            option.apply(pyramid.apply(index)._1()), pyramid.apply(index)._2())) for index in
                  range(0, pyramid.size())}

        image_collection = GeopysparkDataCube(
            pyramid=gps.Pyramid(levels),
            metadata=metadata
        )

        return image_collection.filter_bands(band_indices) if band_indices else image_collection

    def load_ml_model(self, model_id: str) -> 'JavaObject':
        
        def _create_model_dir():
            def _set_permissions(job_dir: Path):
                if not ConfigParams().is_kube_deploy:
                    try:
                        shutil.chown(job_dir, user = None, group = 'eodata')
                    except LookupError as e:
                        logger.warning(f"Could not change group of {job_dir} to eodata.")
                add_permissions(job_dir, stat.S_ISGID | stat.S_IWGRP)  # make children inherit this group
            ml_models_path = GpsBatchJobs.get_job_output_dir('ml_models')
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
            directory = GpsBatchJobs.get_job_output_dir(model_id)
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
        if isinstance(error, Py4JJavaError):
            java_exception = error.java_exception

            while java_exception.getCause() is not None and java_exception != java_exception.getCause():
                java_exception = java_exception.getCause()

            java_exception_class_name = java_exception.getClass().getName()
            java_exception_message = java_exception.getMessage()

            no_data_found = (java_exception_class_name == 'java.lang.AssertionError'
                             and "Cannot stitch empty collection" in java_exception_message)

            is_client_error = java_exception_class_name == 'java.lang.IllegalArgumentException' or no_data_found
            summary = "Cannot construct an image because the given boundaries resulted in an empty image collection" if no_data_found else java_exception_message

            return ErrorSummary(error, is_client_error, summary)

        return error

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

    def after_request(self):
        sc = SparkContext.getOrCreate()
        jvm = sc._gateway.jvm

        for mdc_key in [jvm.org.openeo.logging.JsonLayout.RequestId(), jvm.org.openeo.logging.JsonLayout.UserId()]:
            mdc_remove(sc, jvm, mdc_key)

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self.batch_jobs.set_default_sentinel_hub_credentials(client_id, client_secret)
        self.catalog.set_default_sentinel_hub_credentials(client_id, client_secret)

    def set_terrascope_access_token_getter(self, get_terrascope_access_token: Callable[[User, str], str]):
        self.batch_jobs.set_terrascope_access_token_getter(get_terrascope_access_token)


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

                if metadata.get("_vito", "data_source", "check_missing_products", default=None):
                    temporal_extent = constraints.get("temporal_extent")
                    spatial_extent = constraints.get("spatial_extent")
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


class GpsBatchJobs(backend.BatchJobs):
    _OUTPUT_ROOT_DIR = Path("/batch_jobs") if ConfigParams().is_kube_deploy else Path("/data/projects/OpenEO/")

    def __init__(self, catalog: CollectionCatalog, jvm: JVMView, principal: str, key_tab: str, vault: Vault):
        super().__init__()
        self._catalog = catalog
        self._jvm = jvm
        self._principal = principal
        self._key_tab = key_tab
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None
        self._get_terrascope_access_token: Optional[Callable[[User, str], str]] = None
        self._vault = vault

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    def set_terrascope_access_token_getter(self, get_terrascope_access_token: Callable[[User, str], str]):
        self._get_terrascope_access_token = get_terrascope_access_token

    def create_job(
            self, user_id: str, process: dict, api_version: str,
            metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        job_id = generate_unique_id(prefix="j")
        title = metadata.get("title")
        description = metadata.get("description")
        with JobRegistry() as registry:
            job_info = registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version=api_version,
                specification=dict_no_none(
                    process_graph=process["process_graph"],
                    job_options=job_options,
                ),
                title=title, description=description,
            )
        return BatchJobMetadata(
            id=job_id, process=process, status=job_info["status"],
            created=rfc3339.parse_datetime(job_info["created"]), job_options=job_options,
            title=title, description=description,
        )

    def get_job_info(self, job_id: str, user_id: str) -> BatchJobMetadata:
        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)

        return JobRegistry.job_info_to_metadata(job_info)

    def poll_sentinelhub_batch_processes(self, job_info: dict, sentinel_hub_client_alias: str,
                                         vault_token: Optional[str]):
        # TODO: split polling logic and resuming logic?
        job_id, user_id = job_info['job_id'], job_info['user_id']

        def batch_request_details(batch_process_dependency: dict) -> Dict[str, Tuple[str, Callable[[], None]]]:
            """returns an ID -> (status, retrier) for each batch request ID in the dependency"""
            collection_id = batch_process_dependency['collection_id']

            metadata = GeopysparkCubeMetadata(self._catalog.get_collection_metadata(collection_id))
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

            batch_processing_service = self._jvm.org.openeo.geotrellissentinelhub.BatchProcessingService(
                endpoint, bucket_name, sentinel_hub_client_id, sentinel_hub_client_secret,
                ','.join(ConfigParams().zookeepernodes), f"/openeo/rlguard/access_token_{sentinel_hub_client_alias}"
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
            return {request_id: (batch_processing_service.get_batch_process(request_id),
                                 retrier(request_id)) for request_id in batch_request_ids if request_id is not None}

        dependencies = job_info.get('dependencies') or []
        batch_processes = reduce(partial(dict_merge_recursive, overwrite=True),
                                 (batch_request_details(dependency) for dependency in dependencies))

        batch_process_statuses = {batch_request_id: details.status()
                                  for batch_request_id, (details, _) in batch_processes.items()}

        logger.debug("Sentinel Hub batch process statuses for batch job {j}: {ss}"
                     .format(j=job_id, ss=batch_process_statuses), extra={'job_id': job_id, 'user_id': user_id})

        if any(status == "FAILED" for status in batch_process_statuses.values()):  # at least one failed: not recoverable
            with JobRegistry() as registry:
                registry.set_dependency_status(job_id, user_id, 'error')
                registry.set_status(job_id, user_id, 'error')
                registry.mark_done(job_id, user_id)

            job_info['status'] = 'error'  # TODO: avoid mutation
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

            with JobRegistry() as registry:
                batch_process_processing_units = sum(details.processing_units_spent() or Decimal("0.0")
                                                           for details, _ in batch_processes.values())

                logger.debug(f"Total cost of Sentinel Hub batch processes: {batch_process_processing_units} PU",
                             extra={'job_id': job_id, 'user_id': user_id})

                registry.set_dependencies(job_id, user_id, dependencies)
                registry.set_dependency_status(job_id, user_id, 'available')
                registry.set_dependency_usage(job_id, user_id, batch_process_processing_units)

            self._start_job(job_id, user_id, lambda _: vault_token, dependencies)
        elif all(status in ["DONE", "PARTIAL"] for status in batch_process_statuses.values()):  # all done but some partially failed
            if job_info.get('dependency_status') != 'awaiting_retry':  # haven't retried yet: retry
                with JobRegistry() as registry:
                    registry.set_dependency_status(job_id, user_id, 'awaiting_retry')

                retries = [retry for details, retry in batch_processes.values() if details.status() == "PARTIAL"]

                for retry in retries:
                    retry()
                # TODO: the assumption is that a successful /restartpartial request means that processing has
                #  effectively restarted and a different status (PROCESSING) is published; otherwise the next poll might
                #  still see the previous status (PARTIAL), consider it the new status and immediately mark it as
                #  unrecoverable.
            else:  # still some PARTIALs after one retry: not recoverable
                with JobRegistry() as registry:
                    registry.set_dependency_status(job_id, user_id, 'error')
                    registry.set_status(job_id, user_id, 'error')
                    registry.mark_done(job_id, user_id)

                job_info['status'] = 'error'  # TODO: avoid mutation
        else:  # still some in progress and none FAILED yet: continue polling
            pass

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        with JobRegistry() as registry:
            return [
                registry.job_info_to_metadata(job_info)
                for job_info in registry.get_user_jobs(user_id)
            ]

    @staticmethod
    def get_job_output_dir(job_id: str) -> Path:
        return GpsBatchJobs._OUTPUT_ROOT_DIR / job_id

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
            with JobRegistry() as registry:
                # TODO: add dedicated method
                registry.patch(job_id=job_id, user_id=user.user_id, proxy_user=proxy_user)

        # only fetch it when necessary (SHub collection with non-default credentials) and only once
        @lru_cache(maxsize=None)
        def _get_vault_token(sentinel_hub_client_alias: str) -> str:
            terrascope_access_token = self._get_terrascope_access_token(user, sentinel_hub_client_alias)
            return self._vault.login_jwt(terrascope_access_token)

        self._start_job(job_id, user.user_id, _get_vault_token)

    def _start_job(self, job_id: str, user_id: str, get_vault_token: Callable[[str], str],
                   batch_process_dependencies: Union[list, None] = None):
        from openeogeotrellis import async_task  # TODO: avoid local import because of circular dependency

        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)
            api_version = job_info.get('api_version')

            if batch_process_dependencies is None:
                # restart logic
                current_status = job_info['status']

                if current_status in ['queued', 'running']:
                    return
                elif current_status != 'created':  # TODO: not in line with the current spec (it must first be canceled)
                    registry.mark_ongoing(job_id, user_id)
                    registry.set_application_id(job_id, user_id, None)
                    registry.set_status(job_id, user_id, 'created')

            spec = json.loads(job_info['specification'])
            job_title = job_info.get('title', '')
            job_options = spec.get('job_options', {})
            sentinel_hub_client_alias = deep_get(job_options, 'sentinel-hub', 'client-alias', default="default")

            logger.debug("job_options: {o!r}".format(o=job_options), extra={'job_id': job_id, 'user_id': user_id})

            if (batch_process_dependencies is None
                    and job_info.get('dependency_status') not in ['awaiting', 'awaiting_retry', 'available']
                    and self._scheduled_sentinelhub_batch_processes(spec['process_graph'], api_version, registry,
                                                                    user_id, job_id, job_options,
                                                                    sentinel_hub_client_alias, get_vault_token)):
                async_task.schedule_poll_sentinelhub_batch_processes(job_id, user_id, sentinel_hub_client_alias,
                                                                     vault_token=None
                                                                     if sentinel_hub_client_alias == 'default'
                                                                     else get_vault_token(sentinel_hub_client_alias))
                registry.set_dependency_status(job_id, user_id, 'awaiting')
                registry.set_status(job_id, user_id, 'queued')
                return

            def as_boolean_arg(job_option_key: str, default_value: str) -> str:
                value = job_options.get(job_option_key)

                if value is None:
                    return default_value
                elif isinstance(value, str):
                    return value
                elif isinstance(value, bool):
                    return str(value).lower()
                else:
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
                else:
                    raise OpenEOApiException(message=f"invalid value {value} for job_option soft-errors; "
                                                     f"supported values include false/true and values in the "
                                                     f"interval [0.0, 1.0]",
                                             status_code=400)

            driver_memory = job_options.get("driver-memory", "8G")
            driver_memory_overhead = job_options.get("driver-memoryOverhead", "2G")
            executor_memory = job_options.get("executor-memory", "2G")
            executor_memory_overhead = job_options.get("executor-memoryOverhead", "3G")
            driver_cores = str(job_options.get("driver-cores", 5))
            executor_cores = str(job_options.get("executor-cores", 2))
            executor_corerequest = job_options.get("executor-request-cores", "NONE")
            if executor_corerequest == "NONE":
                executor_corerequest = str(int(executor_cores)/2*1000)+"m"
            max_executors = str(job_options.get("max-executors", 100))
            queue = job_options.get("queue", "default")
            profile = as_boolean_arg("profile", default_value="false")
            max_soft_errors_ratio = as_max_soft_errors_ratio_arg()
            task_cpus = str(job_options.get("task-cpus", 1))
            archives = ",".join(job_options.get("udf-dependency-archives", []))
            use_goofys = as_boolean_arg("goofys", default_value="false")
            logging_threshold = job_options.get("logging-threshold", "info")  # TODO: map from OpenEO levels to Log4j/Python log levels?

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

            if not ConfigParams().is_kube_deploy:
                kerberos(self._principal, self._key_tab, self._jvm)

            if ConfigParams().is_kube_deploy:
                import yaml
                import time
                import io

                from jinja2 import Template
                from kubernetes.client.rest import ApiException
                from openeogeotrellis.utils import kube_client, s3_client, truncate_user_id_k8s

                bucket = 'OpenEO-data'
                s3_instance = s3_client()

                s3_instance.create_bucket(Bucket=bucket)

                user_id_truncated = truncate_user_id_k8s(user_id)
                job_id_truncated = truncate_job_id_k8s(job_id)

                output_dir = str(GpsBatchJobs._OUTPUT_ROOT_DIR) + '/' + job_id

                job_specification_file = output_dir + '/job_specification.json'

                jobspec_bytes = str.encode(job_info['specification'])
                file = io.BytesIO(jobspec_bytes)
                s3_instance.upload_fileobj(file, bucket, job_specification_file.strip('/'))

                if api_version:
                    api_version = api_version
                else:
                    api_version = '0.4.0'

                memOverheadBytes = self._jvm.org.apache.spark.util.Utils.byteStringAsBytes(executor_memory_overhead)
                jvmOverheadBytes = self._jvm.org.apache.spark.util.Utils.byteStringAsBytes("512m")
                python_max = memOverheadBytes - jvmOverheadBytes

                eodata_mount = "/eodata2" if use_goofys != "false" else "/eodata"

                jinja_template = pkg_resources.resource_filename('openeogeotrellis.deploy', 'sparkapplication.yaml.j2')
                rendered = Template(open(jinja_template).read()).render(
                    job_name="job-{j}-{u}".format(j=job_id_truncated, u=user_id_truncated),
                    job_specification=job_specification_file,
                    output_dir=output_dir,
                    output_file="out",
                    log_file="stdout",
                    metadata_file=JOB_METADATA_FILENAME,
                    job_id=job_id_truncated,
                    job_id_full=job_id,
                    driver_cores=driver_cores,
                    driver_memory=driver_memory,
                    executor_cores=executor_cores,
                    executor_corerequest=executor_corerequest,
                    executor_memory=executor_memory,
                    executor_memory_overhead=executor_memory_overhead,
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
                    logging_threshold=logging_threshold
                )

                api_instance = kube_client()

                dict_ = yaml.safe_load(rendered)

                try:
                    submit_response = api_instance.create_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", "spark-jobs", "sparkapplications", dict_, pretty=True)

                    status_response = {}
                    retry=0
                    while('status' not in status_response and retry<10):
                        retry+=1
                        time.sleep(10)
                        try:
                            status_response = api_instance.get_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", "spark-jobs", "sparkapplications", "job-{j}-{u}".format(j=job_id_truncated, u=user_id_truncated))
                        except ApiException as e:
                            logger.info("Exception when calling CustomObjectsApi->list_custom_object: %s\n" % e, extra={'job_id': job_id})

                    if('status' not in status_response):
                        logger.warning("invalid status response: {status}".format(status=str(status_response)), extra={'job_id': job_id})
                        registry.set_status(job_id, user_id, 'error')
                    else:
                        application_id = status_response['status']['sparkApplicationId']
                        logger.info("mapped job_id {a} to application ID {b}".format(a=job_id, b=application_id), extra={'job_id': job_id})
                        registry.set_application_id(job_id, user_id, application_id)
                except ApiException as e:
                    logger.error("Exception when calling CustomObjectsApi->list_custom_object: %s\n" % e, extra={'job_id': job_id})
                    registry.set_status(job_id, user_id, 'error')

            else:
                # TODO: remove old submit scripts?
                submit_script = 'submit_batch_job.sh'
                if( pysparkversion.startswith('2.4')):
                    submit_script = 'submit_batch_job_spark24.sh'
                elif(sys.version_info[0]>=3 and sys.version_info[1]>=8):
                    submit_script = 'submit_batch_job_spark3.sh'
                script_location = pkg_resources.resource_filename('openeogeotrellis.deploy', submit_script)

                with tempfile.NamedTemporaryFile(mode="wt",
                                                 encoding='utf-8',
                                                 dir=GpsBatchJobs._OUTPUT_ROOT_DIR,
                                                 prefix="{j}_".format(j=job_id),
                                                 suffix=".in") as temp_input_file, \
                        tempfile.NamedTemporaryFile(mode="wt",
                                                    encoding='utf-8',
                                                    dir=GpsBatchJobs._OUTPUT_ROOT_DIR,
                                                    prefix="{j}_".format(j=job_id),
                                                    suffix=".properties") as temp_properties_file:
                    temp_input_file.write(job_info['specification'])
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

                    try:
                        logger.info("Submitting job: {a!r}".format(a=args), extra={'job_id': job_id})
                        output_string = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
                    except CalledProcessError as e:
                        logger.exception(e, extra={'job_id': job_id})
                        logger.error(e.stdout, extra={'job_id': job_id})
                        logger.error(e.stderr, extra={'job_id': job_id})
                        raise e

                try:
                    # note: a job_id is returned as soon as an application ID is found in stderr, not when the job is finished
                    logger.info(output_string, extra={'job_id': job_id})
                    application_id = self._extract_application_id(output_string)
                    logger.info("mapped job_id %s to application ID %s" % (job_id, application_id),
                                extra={'job_id': job_id})

                    registry.set_application_id(job_id, user_id, application_id)
                    registry.set_status(job_id, user_id, 'queued')
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
    def _scheduled_sentinelhub_batch_processes(self, process_graph: dict, api_version: Union[str, None],
                                               job_registry: JobRegistry, user_id: str, job_id: str,
                                               job_options: dict, sentinel_hub_client_alias: str,
                                               get_vault_token: Callable[[str], str]) -> bool:
        # TODO: reduce code duplication between this and ProcessGraphDeserializer
        from openeo_driver.dry_run import DryRunDataTracer
        from openeo_driver.ProcessGraphDeserializer import convert_node, ENV_DRY_RUN_TRACER
        from openeo.internal.process_graph_visitor import ProcessGraphVisitor

        env = EvalEnv({
            "user": User(user_id),
            "backend_implementation": GeoPySparkBackendImplementation(),
        })

        if api_version:
            env = env.push({"version": api_version})

        top_level_node = ProcessGraphVisitor.dereference_from_node_arguments(process_graph)
        result_node = process_graph[top_level_node]

        dry_run_tracer = DryRunDataTracer()
        convert_node(result_node, env=env.push({ENV_DRY_RUN_TRACER: dry_run_tracer}))

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
                            # TODO: only area of covered regions instead of whole bbox area?
                            return geometries.get_bounding_box_area()
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

                    batch_processing_service = self._jvm.org.openeo.geotrellissentinelhub.BatchProcessingService(
                        endpoint,
                        bucket_name,
                        sentinel_hub_client_id, sentinel_hub_client_secret,
                        ','.join(ConfigParams().zookeepernodes),
                        f"/openeo/rlguard/access_token_{sentinel_hub_client_alias}"
                    )

                    shub_band_names = metadata.band_names

                    if sar_backscatter_arguments and sar_backscatter_arguments.mask:
                        shub_band_names.append('dataMask')

                    if sar_backscatter_arguments and sar_backscatter_arguments.local_incidence_angle:
                        shub_band_names.append('localIncidenceAngle')

                    def metadata_properties() -> Dict[str, Dict[str, object]]:
                        def as_dicts(criteria):
                            return {criterion[0]: criterion[1] for criterion in criteria}  # (operator -> value)

                        return {property_name: as_dicts(criteria) for property_name, criteria in properties_criteria}

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
                            to_hashable(metadata_properties())
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
                                metadata_properties(),
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
                            to_hashable(metadata_properties()),
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
                                    metadata_properties(),
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
                                        metadata_properties(),
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
            job_registry.set_dependencies(job_id, user_id, batch_process_dependencies)
            return True

        return False

    def get_results(self, job_id: str, user_id: str) -> Dict[str, dict]:
        """
        Reads the metadata json file from the job directory 
        and returns information about the output files.

        :param job_id: The id of the job to get the results for.
        :param user_id: The id of the user that started the job.

        :return: A mapping between a filename and a dict containing information about that file.
        """ 
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status != 'finished':
            raise JobNotFinishedException

        job_dir = self.get_job_output_dir(job_id=job_id)

        # TODO: remove this temporary workaround once we get rid of intermediate job_dir and read from S3 directly
        if ConfigParams().is_kube_deploy and not os.path.exists(job_dir):
            from openeogeotrellis.utils import download_s3_dir
            download_s3_dir("OpenEO-data", f"batch_jobs/{job_id}")

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
                asset["output_dir"] = str(job_dir)
                if "bands" in asset:
                    asset["bands"] = [Band(**b) for b in asset["bands"]]
                results_dict[title] = asset

        return results_dict

    def get_results_metadata(self, job_id: str, user_id: str) -> dict:
        """
        Reads the metadata json file from the job directory and returns it.
        """
        metadata_file = self.get_job_output_dir(job_id) / JOB_METADATA_FILENAME

        try:
            with open(metadata_file) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Could not derive result metadata from %s", metadata_file, exc_info=True,
                           extra={'job_id': job_id})

        return {}

    def get_log_entries(self, job_id: str, user_id: str, offset: Optional[str] = None) -> Iterable[dict]:
        # will throw if job doesn't match user
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status in ['created', 'queued']:
            return iter(())

        if not ConfigParams().is_kube_deploy:
            yield from elasticsearch_logs(job_id)

        try:
            with (self.get_job_output_dir(job_id) / "log").open('r') as f:
                log_file_contents = f.read()
            # TODO: provide log line per line, with correct level?
            # TODO: support offset
            if log_file_contents.strip():
                yield {
                    'id': 'error',
                    'level': 'error',
                    'message': log_file_contents
                }
        except FileNotFoundError:  # some context if the Spark job didn't run and therefore didn't create the log file
            yield {
                'id': 'error',
                'level': 'error',
                'message': traceback.format_exc()
            }

    def cancel_job(self, job_id: str, user_id: str):
        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)

        if job_info['status'] in ['created', 'finished', 'error', 'canceled']:
            return

        application_id = job_info['application_id']

        if application_id:  # can be empty if awaiting SHub dependencies (OpenEO status 'queued')
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

                logger.debug("Killed corresponding Spark job {s} for job {j}: {a!r}".format(s=application_id, j=job_id,
                                                                                            a=kill_spark_job.args),
                             extra={'job_id': job_id})
            except CalledProcessError as e:
                logger.warning(
                    "Could not kill corresponding Spark job {s} for job {j}".format(s=application_id, j=job_id),
                    exc_info=e, extra={'job_id': job_id})
            finally:
                with JobRegistry() as registry:
                    registry.set_status(job_id, user_id, 'canceled')
        else:
            with JobRegistry() as registry:
                registry.remove_dependencies(job_id, user_id)
                registry.set_status(job_id, user_id, 'canceled')
                registry.mark_done(job_id, user_id)

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

        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)
            dependency_sources = JobRegistry.get_dependency_sources(job_info)

        if dependency_sources:
            self.delete_batch_process_dependency_sources(job_id, dependency_sources, propagate_errors)

        with JobRegistry() as registry:
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

    def delete_jobs_before(self, upper: datetime) -> None:
        with JobRegistry() as registry:
            jobs_before = registry.get_all_jobs_before(upper)

        for job_info in jobs_before:
            self._delete_job(job_id=job_info['job_id'], user_id=job_info['user_id'], propagate_errors=True)


class _BatchJobError(Exception):
    pass

