import json
import logging
import re
import subprocess
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from subprocess import CalledProcessError
from typing import List, Dict, Union

import geopyspark as gps
import pkg_resources
from geopyspark import TiledRasterLayer, LayerType
from openeo.error_summary import ErrorSummary
from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo.util import dict_no_none
from openeo_driver import backend
from openeo_driver.backend import ServiceMetadata, BatchJobMetadata, OidcProvider
from openeo_driver.errors import JobNotFinishedException, JobNotStartedException
from openeo_driver.utils import parse_rfc3339
from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JJavaError

from openeogeotrellis.GeotrellisImageCollection import GeotrellisTimeSeriesImageCollection
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.geotrellis_tile_processgraph_visitor import GeotrellisTileProcessGraphVisitor
from openeogeotrellis.job_registry import JobRegistry
from openeogeotrellis.layercatalog import get_layer_catalog
from openeogeotrellis.service_registry import InMemoryServiceRegistry, ZooKeeperServiceRegistry, AbstractServiceRegistry
from openeogeotrellis.utils import kerberos
from openeogeotrellis.utils import normalize_date

logger = logging.getLogger(__name__)


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
            batch_jobs=GpsBatchJobs(),
        )

    def health_check(self) -> str:
        from pyspark import SparkContext
        sc = SparkContext.getOrCreate()
        count = sc.parallelize([1, 2, 3]).map(lambda x: x * x).sum()
        return 'Health check: ' + str(count)

    def oidc_providers(self) -> List[OidcProvider]:
        return [
            OidcProvider(
                id="keycloak",
                # TODO EP-3377: move this to config or bootstrap script (and start using production URL?)
                issuer="https://sso-dev.vgt.vito.be/auth/realms/terrascope",
                scopes=["openid"],
                title="VITO Keycloak",
            )
        ]

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

    def visit_process_graph(self, process_graph: dict) -> ProcessGraphVisitor:
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
    _OUTPUT_ROOT_DIR = Path("/data/projects/OpenEO/")

    def create_job(self, user_id: str, process: dict, api_version: str, job_options: dict = None) -> BatchJobMetadata:
        job_id = str(uuid.uuid4())
        with JobRegistry() as registry:
            job_info = registry.register(
                job_id=job_id,
                user_id=user_id,
                api_version=api_version,
                specification=dict_no_none(
                    process_graph=process["process_graph"],
                    job_options=job_options,
                )
            )
        return BatchJobMetadata(
            id=job_id, process=process, status=job_info["status"],
            created=parse_rfc3339(job_info["created"]), job_options=job_options
        )

    def get_job_info(self, job_id: str, user_id: str) -> BatchJobMetadata:
        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)
            return registry.job_info_to_metadata(job_info)

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        with JobRegistry() as registry:
            return [
                registry.job_info_to_metadata(job_info)
                for job_info in registry.get_user_jobs(user_id)
            ]

    def _get_job_output_dir(self, job_id: str) -> Path:
        return GpsBatchJobs._OUTPUT_ROOT_DIR / job_id

    def start_job(self, job_id: str, user_id: str):
        from pyspark import SparkContext

        with JobRegistry() as registry:
            job_info = registry.get_job(job_id, user_id)
            api_version = job_info.get('api_version')

            # restart logic
            current_status = job_info['status']
            if current_status in ['queued', 'running']:
                return
            elif current_status != 'created':
                registry.mark_ongoing(job_id, user_id)
                registry.set_application_id(job_id, user_id, None)
                registry.set_status(job_id, user_id, 'created')

            spec = json.loads(job_info['specification'])
            extra_options = spec.get('job_options', {})

            driver_memory = extra_options.get("driver-memory", "12G")
            executor_memory = extra_options.get("executor-memory", "2G")
            executor_memory_overhead = extra_options.get("executor-memoryOverhead", "2G")
            driver_cores =extra_options.get("driver-cores", "5")
            executor_cores =extra_options.get("executor-cores", "2")

            kerberos()

            output_dir = self._get_job_output_dir(job_id)
            # TODO: how support multiple output files?
            output_file = output_dir / "out"
            log_file = output_dir / "log"

            conf = SparkContext.getOrCreate().getConf()
            principal, key_tab = conf.get("spark.yarn.principal"), conf.get("spark.yarn.keytab")

            script_location = pkg_resources.resource_filename('openeogeotrellis.deploy', 'submit_batch_job.sh')

            with tempfile.NamedTemporaryFile(mode="wt",
                                             encoding='utf-8',
                                             dir=GpsBatchJobs._OUTPUT_ROOT_DIR,
                                             prefix="{j}_".format(j=job_id),
                                             suffix=".in") as temp_input_file:
                temp_input_file.write(job_info['specification'])
                temp_input_file.flush()

                args = [script_location,
                        "OpenEO batch job {j} user {u}".format(j=job_id, u=user_id),
                        temp_input_file.name,
                        str(output_file),
                        str(log_file)]

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

    def get_results(self, job_id: str, user_id: str) -> Dict[str, str]:
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status != 'finished':
            raise JobNotFinishedException
        return {
            "out": str(self._get_job_output_dir(job_id=job_id))
        }

    def get_log_entries(self, job_id: str, user_id: str, offset: str) -> List[dict]:
        # will throw if job doesn't match user
        job_info = self.get_job_info(job_id=job_id, user_id=user_id)
        if job_info.status in ['created', 'queued']:
            raise JobNotStartedException

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
            application_id = registry.get_job(job_id, user_id)['application_id']
        # TODO: better logging of this kill.
        subprocess.run(
            ["yarn", "application", "-kill", application_id],
            timeout=20,
            check=True,
        )


class _BatchJobError(Exception):
    pass
