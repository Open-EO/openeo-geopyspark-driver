import logging
import operator
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
import uuid
from datetime import datetime
from functools import reduce
from pathlib import Path
from shapely.geometry.polygon import Polygon
from subprocess import CalledProcessError
from typing import Dict

import geopyspark as gps
import pkg_resources
from geopyspark import TiledRasterLayer, LayerType

from openeo_driver.users import User
from openeogeotrellis import filter_properties
from openeogeotrellis import sentinel_hub
from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JJavaError

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.metadata import TemporalDimension, SpatialDimension, Band
from openeo.util import dict_no_none, rfc3339
from openeo_driver import backend
from openeo_driver.backend import (ServiceMetadata, BatchJobMetadata, OidcProvider, ErrorSummary, LoadParameters,
                                   CollectionCatalog)
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.errors import (JobNotFinishedException, ProcessGraphMissingException,
                                  OpenEOApiException, InternalException, ServiceUnsupportedException,
                                  FeatureUnsupportedException)
from openeo_driver.utils import EvalEnv
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube, GeopysparkCubeMetadata
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.service_registry import (InMemoryServiceRegistry, ZooKeeperServiceRegistry,
                                               AbstractServiceRegistry, SecondaryService, ServiceEntity)
from openeogeotrellis.traefik import Traefik
from openeogeotrellis.user_defined_process_repository import *
from openeogeotrellis.utils import normalize_date, kerberos, zk_client
from openeogeotrellis._utm import area_in_square_meters

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

    def remove_services_before(self, upper: datetime) -> None:
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

        service_id = str(uuid.uuid4())

        image_collection: GeopysparkDataCube = evaluate(
            process_graph,
            env=EvalEnv({'version': api_version, 'pyramid_levels': 'all'})
        )

        wmts_base_url = os.getenv('WMTS_BASE_URL_PATTERN', 'http://openeo.vgt.vito.be/openeo/services/%s') % service_id

        self.service_registry.persist(user_id, ServiceMetadata(
            id=service_id,
            process={"process_graph": process_graph},
            url=wmts_base_url + "/service/wmts",
            type=service_type,
            enabled=True,
            attributes={},
            configuration=configuration,
            created=datetime.utcnow()), api_version)

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
            env=EvalEnv({'version': api_version, 'pyramid_levels': 'all'})
        )

        wmts_base_url = os.getenv('WMTS_BASE_URL_PATTERN', 'http://openeo.vgt.vito.be/openeo/services/%s') % service_id

        secondary_service = self._wmts_service(image_collection, configuration, wmts_base_url)

        self.service_registry.register(service_id, secondary_service)
        self._proxy_service(service_id, secondary_service.host, secondary_service.port)

    def _wmts_service(self, image_collection, configuration: dict, wmts_base_url: str) -> SecondaryService:
        random_port = 0

        jvm = gps.get_spark_context()._gateway.jvm
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
        for user_id, service_metadata in self.service_registry.get_metadata_all_before(upper=datetime.max):
            if service_metadata.enabled:
                self.start_service(user_id=user_id, service_id=service_metadata.id)

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

    def __init__(self):
        # TODO: do this with a config instead of hardcoding rules?
        self._service_registry = (
            InMemoryServiceRegistry() if ConfigParams().is_ci_context
            else ZooKeeperServiceRegistry()
        )

        user_defined_process_repository = (
            # choosing between DBs can be done in said config
            InMemoryUserDefinedProcessRepository() if ConfigParams().is_ci_context
            else ZooKeeperUserDefinedProcessRepository()
        )

        catalog = get_layer_catalog(opensearch_enrich=True)

        super().__init__(
            secondary_services=GpsSecondaryServices(service_registry=self._service_registry),
            catalog=catalog,
            batch_jobs=GpsBatchJobs(catalog),
            user_defined_processes=UserDefinedProcesses(user_defined_process_repository)
        )

    def health_check(self) -> str:
        from pyspark import SparkContext
        sc = SparkContext.getOrCreate()
        count = sc.parallelize([1, 2, 3]).map(lambda x: x * x).sum()
        return 'Health check: ' + str(count)

    def oidc_providers(self) -> List[OidcProvider]:
        # TODO Move these providers to config or bootstrap script?
        return [
            OidcProvider(
                id="keycloak",
                issuer="https://sso.vgt.vito.be/auth/realms/terrascope",
                scopes=["openid"],
                title="VITO Keycloak",
            ),
            OidcProvider(
                id="egi-dev",
                issuer="https://aai-dev.egi.eu/oidc/",
                scopes=["openid"],
                title="EGI Check-in (dev)"
            ),
            # OidcProvider(
            #     id="egi-demo",
            #     issuer="https://aai-demo.egi.eu/oidc/",
            #     scopes=["openid"],
            #     title="EGI Check-in (demo)"
            # ),
            # OidcProvider(
            #     id="egi",
            #     issuer="https://aai.egi.eu/oidc/",
            #     scopes=["openid"],
            #     title="EGI Check-in"
            # ),
        ]

    def file_formats(self) -> dict:
        return {
            "input": {
                "GeoJSON": {
                    "gis_data_types": ["vector"],
                    "parameters": {},
                }
            },
            "output": {
                "GTiff": {
                    "title": "GeoTiff",
                    "gis_data_types": ["raster"],
                    "parameters": {},
                },
                "PNG": {
                    "title": "Portable Network Graphics",
                    "gis_data_types": ["raster"],
                    "parameters": {}
                },
                "CovJSON": {
                    "title": "CoverageJSON",
                    "gis_data_types": ["other"],  # TODO: also "raster", "vector", "table"?
                    "parameters": {},
                },
                "NetCDF": {
                    "title": "Network Common Data Form",
                    "gis_data_types": ["other","raster"],  # TODO: also "raster", "vector", "table"?
                    "parameters": {},
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
            kerberos()

        metadata = GeopysparkCubeMetadata(metadata={}, dimensions=[
            # TODO: detect actual dimensions instead of this simple default?
            SpatialDimension(name="x", extent=[]), SpatialDimension(name="y", extent=[]),
            TemporalDimension(name='t', extent=[])
        ])

        # TODO: eliminate duplication with GeoPySparkLayerCatalog.load_collection
        temporal_extent = load_params.temporal_extent
        from_date, to_date = [normalize_date(d) for d in temporal_extent]
        metadata = metadata.filter_temporal(from_date, to_date)

        spatial_extent = load_params.spatial_extent
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

        sc = gps.get_spark_context()

        gateway = JavaGateway(eager_load=True, gateway_parameters=sc._gateway.gateway_parameters)
        jvm = gateway.jvm

        extent = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north)) \
            if spatial_bounds_present else None

        pyramid = jvm.org.openeo.geotrellis.geotiff.PyramidFactory.from_disk(glob_pattern, date_regex) \
            .pyramid_seq(extent, crs, from_date, to_date)

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


class GpsBatchJobs(backend.BatchJobs):
    _OUTPUT_ROOT_DIR = Path("/batch_jobs") if ConfigParams().is_kube_deploy else Path("/data/projects/OpenEO/")

    def __init__(self, catalog: CollectionCatalog):
        self._catalog = catalog

    def create_job(
            self, user_id: str, process: dict, api_version: str,
            metadata: dict, job_options: dict = None
    ) -> BatchJobMetadata:
        job_id = str(uuid.uuid4())
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

        # FIXME: for now, the assumption is that the client will call this on a regular basis
        # an external process can poll SHub, schedule a Spark job if all DONE and stop -> no unnecessary invocations
        # but this one has to keep state somewhere to prevent it from doing this
        # TODO: don't do this if called internally
        if job_info.get('dependency_status') == 'awaiting':
            self._poll_sentinelhub_batch_processes(job_id, user_id, job_info)

        return registry.job_info_to_metadata(job_info)

    def _poll_sentinelhub_batch_processes(self, job_id: str, user_id: str, job_info: dict):
        def statuses(batch_process_dependency: dict) -> List[str]:
            collection_id = batch_process_dependency['collection_id']

            metadata = GeopysparkCubeMetadata(self._catalog.get_collection_metadata(collection_id))
            layer_source_info = metadata.get("_vito", "data_source", default={})

            client_id = layer_source_info['client_id']
            client_secret = layer_source_info['client_secret']

            jvm = gps.get_spark_context()._gateway.jvm
            batch_processing_service = jvm.org.openeo.geotrellissentinelhub.BatchProcessingService(
                ConfigParams().sentinel_hub_batch_bucket, client_id, client_secret)

            batch_request_ids = (batch_process_dependency.get('batch_request_ids') or
                                 [batch_process_dependency['batch_request_id']])

            return [batch_processing_service.get_batch_process_status(request_id) for request_id in batch_request_ids]

        dependencies = job_info.get('dependencies') or []
        statuses = set(reduce(operator.add, (statuses(batch_process) for batch_process in dependencies)))

        logger.debug("Sentinel Hub batch process statuses for batch job {j}: {ss}".format(j=job_id, ss=statuses))

        if statuses == {"DONE"}:
            with JobRegistry() as registry:
                registry.set_dependency_status(job_id, user_id, 'available')

            self._start_job(job_id, user_id, dependencies)  # resume batch job with now available data
        elif "FAILED" in statuses:
            with JobRegistry() as registry:
                registry.set_dependency_status(job_id, user_id, 'error')
                registry.set_status(job_id, user_id, 'error')
                registry.mark_done(job_id, user_id)

            job_info['status'] = 'error'  # TODO: avoid mutation

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        with JobRegistry() as registry:
            return [
                registry.job_info_to_metadata(job_info)
                for job_info in registry.get_user_jobs(user_id)
            ]

    def _get_job_output_dir(self, job_id: str) -> Path:
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

    def start_job(self, job_id: str, user_id: str):
        self._start_job(job_id, user_id)

    def _start_job(self, job_id: str, user_id: str, batch_process_dependencies: Union[list, None] = None):
        from pyspark import SparkContext

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
            extra_options = spec.get('job_options', {})

            if (batch_process_dependencies is None
                    and job_info.get('dependency_status') not in ['awaiting', 'available']
                    and self._scheduled_sentinelhub_batch_processes(spec['process_graph'], api_version, registry,
                                                                    user_id, job_id)):
                registry.set_dependency_status(job_id, user_id, 'awaiting')
                registry.set_status(job_id, user_id, 'queued')
                return

            driver_memory = extra_options.get("driver-memory", "12G")
            driver_memory_overhead = extra_options.get("driver-memoryOverhead", "2G")
            executor_memory = extra_options.get("executor-memory", "2G")
            executor_memory_overhead = extra_options.get("executor-memoryOverhead", "2G")
            driver_cores =extra_options.get("driver-cores", "5")
            executor_cores =extra_options.get("executor-cores", "2")
            queue = extra_options.get("queue", "default")
            profile = extra_options.get("profile", "false")

            def serialize_dependencies():
                dependencies = batch_process_dependencies or job_info.get('dependencies') or []

                if not dependencies:
                    return 'no_dependencies'  # TODO: clean this up

                pairs = ["{c}:{f}:{m}".format(c=dependency['collection_id'],
                                              f=dependency.get('subfolder') or dependency['batch_request_id'],
                                              m=dependency.get('card4l', False))
                         for dependency in dependencies]

                return ",".join(pairs)

            if not ConfigParams().is_kube_deploy:
                kerberos()

            conf = SparkContext.getOrCreate().getConf()

            principal, key_tab = conf.get("spark.yarn.principal",conf.get("spark.kerberos.principal")), conf.get("spark.yarn.keytab",conf.get("spark.kerberos.keytab"))

            if ConfigParams().is_kube_deploy:
                import yaml
                import time
                import io

                from jinja2 import Template
                from kubernetes.client.rest import ApiException
                from openeogeotrellis.utils import kube_client, s3_client

                bucket = 'OpenEO-data'
                s3_instance = s3_client()

                s3_instance.create_bucket(Bucket=bucket)

                output_dir = str(GpsBatchJobs._OUTPUT_ROOT_DIR) + '/' + job_id

                job_specification_file = output_dir + '/job_specification.json'

                jobspec_bytes = str.encode(job_info['specification'])
                file = io.BytesIO(jobspec_bytes)
                s3_instance.upload_fileobj(file, bucket, job_specification_file.strip('/'))

                if api_version:
                    api_version = api_version
                else:
                    api_version = '0.4.0'

                jinja_template = pkg_resources.resource_filename('openeogeotrellis.deploy', 'sparkapplication.yaml.j2')
                rendered = Template(open(jinja_template).read()).render(
                    job_name="job-{j}-{u}".format(j=job_id, u=user_id),
                    job_specification=job_specification_file,
                    output_dir=output_dir,
                    output_file="out",
                    log_file="log",
                    metadata_file=JOB_METADATA_FILENAME,
                    job_id=job_id,
                    driver_cores=driver_cores,
                    driver_memory=driver_memory,
                    executor_cores=executor_cores,
                    executor_memory=executor_memory,
                    api_version=api_version,
                    current_time=int(time.time()),
                    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                    swift_url=os.environ.get("SWIFT_URL"),
                    image_name=os.environ.get("IMAGE_NAME"),
                    swift_bucket=bucket,
                    zookeeper_nodes=os.environ.get("ZOOKEEPERNODES")
                )

                api_instance = kube_client()

                dict = yaml.safe_load(rendered)

                try:
                    submit_response = api_instance.create_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", "spark-jobs", "sparkapplications", dict, pretty=True)

                    time.sleep(5)
                    status_response = api_instance.get_namespaced_custom_object("sparkoperator.k8s.io", "v1beta2", "spark-jobs", "sparkapplications", "job-{j}-{u}".format(j=job_id, u=user_id))
                    application_id = status_response['status']['sparkApplicationId']

                    logger.info("mapped job_id {a} to application ID {b}".format(a=job_id, b=application_id))
                    registry.set_application_id(job_id, user_id, application_id)
                except ApiException as e:
                    print("Exception when calling CustomObjectsApi->list_custom_object: %s\n" % e)

            else:
                submit_script = 'submit_batch_job.sh'
                if(sys.version_info[0]>=3 and sys.version_info[1]>=8):
                    submit_script = 'submit_batch_job_spark3.sh'
                script_location = pkg_resources.resource_filename('openeogeotrellis.deploy', submit_script)

                with tempfile.NamedTemporaryFile(mode="wt",
                                                 encoding='utf-8',
                                                 dir=GpsBatchJobs._OUTPUT_ROOT_DIR,
                                                 prefix="{j}_".format(j=job_id),
                                                 suffix=".in") as temp_input_file:
                    temp_input_file.write(job_info['specification'])
                    temp_input_file.flush()

                    # TODO: implement a saner way of passing arguments
                    args = [script_location,
                            "OpenEO batch job {j} user {u}".format(j=job_id, u=user_id),
                            temp_input_file.name,
                            str(self._get_job_output_dir(job_id)),
                            "out",  # TODO: how support multiple output files?
                            "log",
                            JOB_METADATA_FILENAME,
                            ]

                    if principal is not None and key_tab is not None:
                        args.append(principal)
                        args.append(key_tab)
                    else:
                        args.append("no_principal")
                        args.append("no_keytab")

                    args.append(user_id)

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

                    try:
                        logger.info("Submitting job: {a!r}".format(a=args))
                        output_string = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True)
                    except CalledProcessError as e:
                        logger.exception(e)
                        logger.error(e.stdout)
                        logger.error(e.stderr)
                        raise e

                try:
                    # note: a job_id is returned as soon as an application ID is found in stderr, not when the job is finished
                    logger.info(output_string)
                    application_id = self._extract_application_id(output_string)
                    print("mapped job_id %s to application ID %s" % (job_id, application_id))

                    registry.set_application_id(job_id, user_id, application_id)
                    registry.set_status(job_id, user_id, 'queued')
                except _BatchJobError as e:
                    traceback.print_exc(file=sys.stderr)
                    # TODO: why reraise as CalledProcessError?
                    raise CalledProcessError(1, str(args), output=output_string)

    @staticmethod
    def _extract_application_id(stream) -> str:
        regex = re.compile(r"^.*Application report for (application_\d{13}_\d+)\s\(state:.*", re.MULTILINE)
        match = regex.search(stream)
        if match:
            return match.group(1)
        else:
            raise _BatchJobError(stream)

    def _scheduled_sentinelhub_batch_processes(self, process_graph: dict, api_version: Union[str, None],
                                               job_registry: JobRegistry, user_id: str, job_id: str) -> bool:
        # TODO: reduce code duplication between this and ProcessGraphDeserializer
        from openeo_driver.dry_run import DryRunDataTracer
        from openeo_driver.ProcessGraphDeserializer import convert_node, ENV_DRY_RUN_TRACER
        from openeo.internal.process_graph_visitor import ProcessGraphVisitor

        env = EvalEnv({
            "user": User(user_id)
        })

        if api_version:
            env = env.push({"version": api_version})

        top_level_node = ProcessGraphVisitor.dereference_from_node_arguments(process_graph)
        result_node = process_graph[top_level_node]

        dry_run_tracer = DryRunDataTracer()
        convert_node(result_node, env=env.push({ENV_DRY_RUN_TRACER: dry_run_tracer}))

        source_constraints = dry_run_tracer.get_source_constraints()
        logger.info("Dry run extracted these source constraints: {s}".format(s=source_constraints))

        batch_process_dependencies = []

        for (process, arguments), constraints in source_constraints.items():
            if process == 'load_collection':
                collection_id, = arguments
                band_names = constraints.get('bands')

                metadata = GeopysparkCubeMetadata(self._catalog.get_collection_metadata(collection_id))
                if band_names:
                    metadata = metadata.filter_bands(band_names)

                layer_source_info = metadata.get("_vito", "data_source")

                if ("sar_backscatter" in constraints
                        and not layer_source_info.get("sar_backscatter_compatible", False)):
                    raise OpenEOApiException(message=
                                             """Process "sar_backscatter" is not applicable for collection {c}."""
                                             .format(c=collection_id), status_code=400)

                if layer_source_info['type'] == 'sentinel-hub':
                    sar_backscatter_arguments = constraints.get("sar_backscatter", SarBackscatterArgs())

                    card4l = (sar_backscatter_arguments.coefficient == "gamma0-terrain"
                              and sar_backscatter_arguments.mask
                              and sar_backscatter_arguments.local_incidence_angle)

                    spatial_extent = constraints['spatial_extent']

                    def area():
                        geom = Polygon.from_bounds(
                            xmin=spatial_extent['west'],
                            ymin=spatial_extent['south'],
                            xmax=spatial_extent['east'],
                            ymax=spatial_extent['north'])

                        return area_in_square_meters(geom, spatial_extent['crs'])

                    if card4l:
                        logger.info("deemed batch job {j} CARD4L compliant ({s})".format(j=job_id,
                                                                                         s=sar_backscatter_arguments))
                    elif area() >= 50 * 1000 * 50 * 1000:  # 50x50 km
                        logger.info("deemed batch job {j} AOI large enough ({a} m²)".format(j=job_id, a=area()))
                    else:
                        continue  # skip SHub batch process and use sync approach instead

                    jvm = gps.get_spark_context()._gateway.jvm

                    sample_type = jvm.org.openeo.geotrellissentinelhub.SampleType.withName(
                        layer_source_info.get('sample_type', 'UINT16'))

                    from_date, to_date = [normalize_date(d) for d in constraints['temporal_extent']]

                    west = spatial_extent['west']
                    south = spatial_extent['south']
                    east = spatial_extent['east']
                    north = spatial_extent['north']
                    bbox = jvm.geotrellis.vector.Extent(float(west), float(south), float(east), float(north))

                    batch_processing_service = jvm.org.openeo.geotrellissentinelhub.BatchProcessingService(
                        ConfigParams().sentinel_hub_batch_bucket,
                        layer_source_info['client_id'], layer_source_info['client_secret'])

                    shub_band_names = metadata.band_names

                    if sar_backscatter_arguments.mask:
                        shub_band_names.append('dataMask')

                    if sar_backscatter_arguments.local_incidence_angle:
                        shub_band_names.append('localIncidenceAngle')

                    def metadata_properties() -> Dict[str, object]:
                        layer_properties = metadata.get("_vito", "properties", default={})
                        custom_properties = constraints.get("properties", {})

                        return {property_name: filter_properties.extract_literal_match(condition)
                                for property_name, condition in {**layer_properties, **custom_properties}.items()}

                    if card4l:
                        # TODO: not obvious but this does the validation as well
                        dem_instance = sentinel_hub.processing_options(sar_backscatter_arguments).get('demInstance')

                        # cannot be the batch job ID because results for multiple collections would end up in
                        #  the same S3 dir
                        request_group_id = str(uuid.uuid4())
                        subfolder = request_group_id

                        # return type py4j.java_collections.JavaList is not JSON serializable
                        batch_request_ids = list(batch_processing_service.start_card4l_batch_processes(
                            layer_source_info['collection_id'],
                            layer_source_info['dataset_id'],
                            bbox,
                            spatial_extent['crs'],
                            from_date,
                            to_date,
                            shub_band_names,
                            dem_instance,
                            metadata_properties(),
                            subfolder,
                            request_group_id)
                        )
                    else:
                        # TODO: pass subfolder explicitly (also a random UUID) instead of implicit batch request ID?
                        batch_request_ids = [batch_processing_service.start_batch_process(
                            layer_source_info['collection_id'],
                            layer_source_info['dataset_id'],
                            bbox,
                            spatial_extent['crs'],
                            from_date,
                            to_date,
                            shub_band_names,
                            sample_type,
                            metadata_properties(),
                            sentinel_hub.processing_options(sar_backscatter_arguments)
                        )]

                        subfolder = batch_request_ids[0]

                    logger.info("scheduled Sentinel Hub batch process(es) {bs} for batch job {j} (CARD4L {c})"
                                .format(bs=batch_request_ids, j=job_id, c="enabled" if card4l else "disabled"))

                    batch_process_dependencies.append({
                        'collection_id': collection_id,
                        'batch_request_ids': batch_request_ids,  # to poll SHub
                        'subfolder': subfolder,  # where load_collection gets its data
                        'card4l': card4l  # should the batch job expect CARD4L metadata?
                    })

        if batch_process_dependencies:
            job_registry.add_dependencies(job_id, user_id, batch_process_dependencies)
            return True

        return False

    def get_results(self, job_id: str, user_id: str) -> Dict[str, dict]:
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status != 'finished':
            raise JobNotFinishedException
        job_dir = self._get_job_output_dir(job_id=job_id)

        out_metadata = self.get_results_metadata(job_id, user_id).get("assets", {}).get("out", {})
        bands = [Band(*properties) for properties in out_metadata.get("bands", [])]
        nodata = out_metadata.get("nodata", None)
        media_type = out_metadata.get("media_type", "application/octet-stream")

        results_dict = {}

        if os.path.isfile(job_dir / 'out'):
            results_dict['out'] = {
                # TODO: give meaningful filename and extension
                "output_dir": str(job_dir),
                "media_type": media_type,
                "bands": bands,
                "nodata": nodata
            }

        if os.path.isfile(job_dir / 'profile_dumps.tar.gz'):
            results_dict['profile_dumps.tar.gz'] = {
                "output_dir": str(job_dir),
                "media_type": "application/gzip"
            }

        for file_name in os.listdir(job_dir):
            if file_name.endswith("_metadata.json") and file_name != JOB_METADATA_FILENAME:
                results_dict[file_name] = {
                    "output_dir": str(job_dir),
                    "media_type": "application/json"
                }
            elif file_name.endswith("_MULTIBAND.tif"):
                results_dict[file_name] = {
                    "output_dir": str(job_dir),
                    "media_type": "image/tiff; application=geotiff"
                }

        return results_dict

    def get_results_metadata(self, job_id: str, user_id: str) -> dict:
        metadata_file = self._get_job_output_dir(job_id) / JOB_METADATA_FILENAME

        try:
            with open(metadata_file) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Could not derive result metadata from %s", metadata_file, exc_info=True)

        return {}

    def get_log_entries(self, job_id: str, user_id: str, offset: str) -> List[dict]:
        # will throw if job doesn't match user
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status in ['created', 'queued']:
            return []

        log_file = self._get_job_output_dir(job_id) / "log"
        with log_file.open('r') as f:
            log_file_contents = f.read()
        # TODO: provide log line per line, with correct level?
        return [
            {
                'id': "0",
                'level': 'error',
                'message': log_file_contents
            }
        ]

    def cancel_job(self, job_id: str, user_id: str):
        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)

        if job_info['status'] in ['created', 'finished', 'error', 'canceled']:
            return

        application_id = job_info['application_id']

        if application_id:
            kill_spark_job = subprocess.run(
                ["yarn", "application", "-kill", application_id],
                timeout=20,
                check=True,
                universal_newlines=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT  # combine both output streams into one
            )

            with JobRegistry() as registry:
                registry.set_status(job_id, user_id, 'canceled')
            logger.debug("Killed corresponding Spark job for job {j}: {a!r}".format(j=job_id, a=kill_spark_job.args))
        else:
            raise InternalException("Application ID unknown for job {j}".format(j=job_id))

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
                               exc_info=e)

        job_dir = self._get_job_output_dir(job_id)

        try:
            shutil.rmtree(job_dir)
        except FileNotFoundError as e:  # nothing to delete, not an error
            pass
        except Exception as e:
            if propagate_errors:
                raise
            else:
                logger.warning("Could not delete {p}".format(p=job_dir), exc_info=e)

        with JobRegistry() as registry:
            registry.delete(job_id, user_id)

        logger.info("Deleted job {u}/{j}".format(u=user_id, j=job_id))

    def delete_jobs_before(self, upper: datetime) -> None:
        with JobRegistry() as registry:
            jobs_before = registry.get_all_jobs_before(upper)

        for job_info in jobs_before:
            self._delete_job(job_id=job_info['job_id'], user_id=job_info['user_id'], propagate_errors=True)


class _BatchJobError(Exception):
    pass


class UserDefinedProcesses(backend.UserDefinedProcesses):
    _valid_process_graph_id = re.compile(r"^\w+$")

    def __init__(self, user_defined_process_repository: UserDefinedProcessRepository):
        self._repo = user_defined_process_repository

    def get(self, user_id: str, process_id: str) -> Union[UserDefinedProcessMetadata, None]:
        return self._repo.get(user_id, process_id)

    def get_for_user(self, user_id: str) -> List[UserDefinedProcessMetadata]:
        return self._repo.get_for_user(user_id)

    def save(self, user_id: str, process_id: str, spec: dict) -> None:
        self._validate(spec, process_id)
        self._repo.save(user_id, spec)

    def delete(self, user_id: str, process_id: str) -> None:
        self._repo.delete(user_id, process_id)

    def _validate(self, spec: dict, process_id: str) -> None:
        if 'process_graph' not in spec:
            raise ProcessGraphMissingException

        if not self._valid_process_graph_id.match(process_id):
            raise OpenEOApiException(
                status_code=400,
                message="Invalid process_graph_id {i}, must match {p}".format(i=process_id,
                                                                              p=self._valid_process_graph_id.pattern))

        spec['id'] = process_id
