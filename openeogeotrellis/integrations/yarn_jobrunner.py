import json
import logging
import os
import re
import stat
import subprocess
import sys
import tempfile
import traceback
from dataclasses import dataclass
from pathlib import Path
from subprocess import CalledProcessError
from typing import Optional, Union

import pkg_resources
from openeo.util import deep_get, ensure_dir
from openeo_driver.config.load import ConfigGetter
from openeo_driver.errors import InternalException, OpenEOApiException

import openeogeotrellis.integrations.freeipa
from openeogeotrellis import sentinel_hub
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.job_options import JobOptions
from openeogeotrellis.udf import UDF_PYTHON_DEPENDENCIES_ARCHIVE_NAME, UDF_PYTHON_DEPENDENCIES_FOLDER_NAME
from openeogeotrellis.util.byteunit import byte_string_as
from openeogeotrellis.utils import add_permissions

_log = logging.getLogger(__name__)

JOB_METADATA_FILENAME = "job_metadata.json"


@dataclass
class BatchJobSubmitArgs:
    """
    Arguments passed to the submit batch job bash script.
    
    This dataclass represents all positional arguments that are passed to the YARN
    batch job submission script in the correct order.
    """
    
    script_location: str
    """Path to the bash script"""
    
    job_name: str
    """Human-readable name for the batch job (appears in YARN UI)"""
    
    process_graph_file: str
    """Path to temporary file containing the job specification JSON"""
    
    output_dir: str
    """
    Directory where job results will be written.
    Should be a shared directory that is accessible by both the webapp driver and the batch job driver via e.g. NFS.
    Note that the 'webapp' driver is the spark driver that created this dataclass and runs the submit script.
    """
    
    output_file_name: str
    """
    Name of the main output file (usually 'out'). 
    In most cases, such as when writing the assets of a `GeopysparkDataCube`, this filename is ignored.
    """
    
    metadata_file_name: str
    """Name of the job metadata file (usually 'job_metadata.json')"""
    
    principal: str
    """
    Kerberos principal for YARN authentication, or 'no_principal' if not used.
    Required to submit spark jobs to the YARN cluster.
    """
    
    key_tab: str
    """
    Path to Kerberos keytab file for YARN authentication, or 'no_keytab' if not used.
    Required to submit spark jobs to the YARN cluster.
    """

    proxy_user: str
    """
    FreeIPA user to use as submitter of this yarn job. This appears in the YARN UI.
    It decides which directories the driver and executors can read/write to and scheduling priority.
    """
    
    api_version: str
    """OpenEO API version for the job (e.g., '1.0.0')"""
    
    driver_memory: str
    """Spark driver memory allocation (e.g., '8G')"""
    
    executor_memory: str
    """Spark executor memory allocation (e.g., '4G')"""
    
    executor_memory_overhead: str
    """Additional memory overhead for executors (e.g., '3G')"""
    
    driver_cores: str
    """Number of CPU cores for the Spark driver"""
    
    executor_cores: str
    """Number of CPU cores per Spark executor"""
    
    driver_memory_overhead: str
    """Additional memory overhead for the driver (e.g., '8G')"""
    
    queue: str
    """YARN queue to submit the job to (e.g., 'default')"""
    
    profile: str
    """Whether to enable Spark profiling ('true' or 'false'). Passed to `--conf spark.python.profile`."""
    
    dependencies: str
    """JSON string of batch job dependencies for input data"""
    
    py_files: str
    """
    Comma-separated list of .py, .zip, or .egg files to distribute to driver and executors so that they can be imported.
    Mainly for libraries that are not already available in the image.
    We only use this to import `custom_processes.py`.
    See https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management
    """
    
    max_executors: str
    """Maximum number of dynamic executors for the job, e.g. "100"."""
    
    user_id: str
    """
    OpenEO user ID who submitted the job.
    """
    
    batch_job_id: str
    """Unique batch job identifier. E.g. "j-25082114030147d69f692c3ae21c7a1a"."""
    
    max_soft_errors_ratio: str
    """Maximum ratio of soft errors allowed before job failure."""
    
    task_cpus: str
    """Number of CPU cores per Spark task."""
    
    sentinel_hub_client_alias: str
    """
    Alias for Sentinel Hub client configuration.
    TODO: more information needed
    """
    
    properties_file: str
    """Path to temporary file containing sensitive Spark properties."""
    
    udf_dependency_archives: str
    """Comma-separated list of UDF dependency archive paths."""
    
    log_level: str
    """Logging level for the job (e.g., 'ERROR', 'INFO', 'DEBUG')."""
    
    openeo_backend_config: str
    """
    Path or configuration for OpenEO backend settings. 
    This is a private file that contains all the information to fill `config.config.GpsBackendConfig`.
    """
    
    udf_python_dependencies_folder_path: str
    """
    Path to folder containing UDF Python dependencies.
    This feature allows users to provide a list of their own python dependencies for UDFs (executors).
    Note: This shared folder should be available on the batch job driver and executors via e.g. NFS.
    """
    
    ejr_api: str
    """
    Elastic Job Registry API endpoint URL. Passed to `OPENEO_EJR_API` on the driver.
    This registry is used to track job metadata and update the job status.
    Note that this is not a direct connection with elastic but with a custom REST API on top of it.
    """
    
    ejr_backend_id: str
    """
    Elastic Job Registry backend identifier. Passed to `OPENEO_EJR_BACKEND_ID` on the driver.
    Used for the `backend_id` column of this batch job in the Elastic Job registry.
    E.g. 'mep-dev', 'mep-prod'.
    """
    
    ejr_oidc_client_credentials: str
    """Used for authentication with the EJR. Passed to `OPENEO_EJR_OIDC_CLIENT_CREDENTIALS` on the driver."""
    
    docker_mounts: str
    """
    Directories that will be mounted into the batch job driver and executors. 
    These directories have to be available locally on all nodes in the YARN cluster.
    Usually:
    * a set of NFS directories containing raster data, shared batch job data, etc.
    * A set of configuration files for e.g. Kerberos, Hadoop, etc.
    Passed to: 
    `--conf spark.yarn.appMasterEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS`
    `--conf spark.executorEnv.YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS`
    See https://hadoop.apache.org/docs/r3.1.3/hadoop-yarn/hadoop-yarn-site/DockerContainers.html
    """
    
    udf_python_dependencies_archive_path: str
    """Path to UDF Python dependencies archive file."""
    
    propagatable_web_app_driver_envars: str
    """Environment variables to propagate from web app to batch job driver."""
    
    python_max_memory: str
    """
    Maximum memory allocation for Python processes ('-1' for unlimited).
    Passed to `--conf spark.executor.pyspark.memory={python_max_memory}b`.
    See https://spark.apache.org/docs/latest/configuration.html
    """
    
    spark_eventlog_dir: str
    """Directory for Spark event logs"""
    
    spark_history_fs_logdirectory: str
    """Directory for Spark history server logs"""
    
    spark_yarn_historyserver_address: str
    """Address of Spark history server"""
    
    yarn_container_runtime_docker_client_config: str
    """Path to Docker client configuration for YARN containers (optional)"""

    def to_args_list(self) -> list[str]:
        """Convert the dataclass to a list of string arguments for subprocess."""
        return [
            self.script_location,
            self.job_name,
            self.process_graph_file,
            self.output_dir,
            self.output_file_name,
            self.metadata_file_name,
            self.principal,
            self.key_tab,
            self.proxy_user,
            self.api_version,
            self.driver_memory,
            self.executor_memory,
            self.executor_memory_overhead,
            self.driver_cores,
            self.executor_cores,
            self.driver_memory_overhead,
            self.queue,
            self.profile,
            self.dependencies,
            self.py_files,
            self.max_executors,
            self.user_id,
            self.batch_job_id,
            self.max_soft_errors_ratio,
            self.task_cpus,
            self.sentinel_hub_client_alias,
            self.properties_file,
            self.udf_dependency_archives,
            self.log_level,
            self.openeo_backend_config,
            self.udf_python_dependencies_folder_path,
            self.ejr_api,
            self.ejr_backend_id,
            self.ejr_oidc_client_credentials,
            self.docker_mounts,
            self.udf_python_dependencies_archive_path,
            self.propagatable_web_app_driver_envars,
            self.python_max_memory,
            self.spark_eventlog_dir,
            self.spark_history_fs_logdirectory,
            self.spark_yarn_historyserver_address,
            self.yarn_container_runtime_docker_client_config,
        ]


class YARNBatchJobRunner:
    def __init__(self, principal: str = None, key_tab: str = None):
        self._principal = principal
        self._key_tab = key_tab
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    def serialize_dependencies(self, job_info, dependencies=None) -> str:
        batch_process_dependencies = [
            dependency
            for dependency in (dependencies or job_info.get("dependencies") or [])
            if "collection_id" in dependency
        ]

        def as_arg_element(dependency: dict) -> dict:
            source_location = (
                dependency.get("assembled_location")  # cached
                or dependency.get("results_location")  # not cached
                or f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}"
                f"/{dependency.get('subfolder') or dependency['batch_request_id']}"
            )  # legacy

            return {"source_location": source_location, "card4l": dependency.get("card4l", False)}

        return json.dumps([as_arg_element(dependency) for dependency in batch_process_dependencies])

    def _write_sensitive_values(self, output_file, vault_token: Optional[str]):
        output_file.write(f"spark.openeo.sentinelhub.client.id.default={self._default_sentinel_hub_client_id}\n")
        output_file.write(
            f"spark.openeo.sentinelhub.client.secret.default={self._default_sentinel_hub_client_secret}\n"
        )

        if vault_token is not None:
            output_file.write(f"spark.openeo.vault.token={vault_token}\n")

    @staticmethod
    def _extract_application_id(script_output: str) -> str:
        regex = re.compile(r"^.*Application report for (application_\d{13}_\d+)\s\(state:.*", re.MULTILINE)
        match = regex.search(script_output)
        if match:
            return match.group(1)
        else:
            raise _BatchJobError(script_output)

    @staticmethod
    def get_submit_py_files(env: dict = None, cwd: Union[str, Path] = ".", log=None) -> str:
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
                elif log is not None:
                    log.warning(f"Could not find 'py-file' {filename}: skipping")
            py_files = ",".join(found)
        return py_files

    def _verify_proxy_user(self, user: str) -> str:
        """
        Helper to verify if given user id/name is valid
        (to be used with `--proxy-user` option in YARN submit script).

        returns the user_id if it is valid, otherwise an empty string.
        """
        try:
            if cred_info := get_backend_config().freeipa_default_credentials_info:
                gssapi_creds = openeogeotrellis.integrations.freeipa.acquire_gssapi_creds(
                    principal=cred_info["principal"],
                    keytab_path=cred_info["keytab_path"],
                )
            else:
                gssapi_creds = None
            ipa_server = (
                get_backend_config().freeipa_server
                or openeogeotrellis.integrations.freeipa.get_freeipa_server_from_env()
            )
            if ipa_server:
                ipa_client = openeogeotrellis.integrations.freeipa.FreeIpaClient(
                    ipa_server=ipa_server,
                    verify_tls=False,  # TODO?
                    gssapi_creds=gssapi_creds,
                )
                if ipa_client.user_find(user):
                    _log.info(f"_verify_proxy_user: valid {user!r}")
                    return user
        except Exception as e:
            _log.warning(f"Failed to verify whether {user!r} can be used as proxy user: {e!r}")

        _log.info(f"_verify_proxy_user: invalid {user!r}")
        return ""

    def run_job(
        self,
        job_info: dict,
        job_id: str,
        job_work_dir,
        log,
        user_id="",
        api_version="1.0.0",  # TODO: this default is probably not correct, use OPENEO_API_VERSION_DEFAULT instead?
        proxy_user: str = None,
        vault_token: Optional[str] = None,
    ):
        job_process_graph = job_info["process"]["process_graph"]
        job_options = job_info.get("job_options") or {}  # can be None
        job_specification_json = json.dumps({"process_graph": job_process_graph, "job_options": job_options})

        job_title = job_info.get("title", "")
        options = JobOptions.from_dict(job_options)

        ensure_dir(job_work_dir)
        # Ensure others can read/write so that the batch job driver and executors can write to it.
        # The intention is that a cronjob will later only allow the webapp driver to read the results.
        add_permissions(job_work_dir, stat.S_IRWXO | stat.S_IWGRP)

        def as_boolean_arg(job_option_key: str, default_value: str) -> str:
            value = job_options.get(job_option_key)

            if value is None:
                return default_value
            elif isinstance(value, str):
                return value
            elif isinstance(value, bool):
                return str(value).lower()

            raise OpenEOApiException(f"invalid value {value} for job_option {job_option_key}")

        queue = job_options.get("queue", "default")
        profile = as_boolean_arg("profile", default_value="false")
        sentinel_hub_client_alias = deep_get(job_options, "sentinel-hub", "client-alias", default="default")

        submit_script = "submit_batch_job_spark3.sh"
        script_location = pkg_resources.resource_filename("openeogeotrellis.deploy", submit_script)

        image_name = options.image_name or os.environ.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE")
        if image_name:
            image_name = get_backend_config().batch_runtime_to_image.get(image_name.lower(), image_name)

        extra_py_files = ""
        if options.udf_dependency_files is not None and len(options.udf_dependency_files) > 0:
            extra_py_files = ",".join(options.udf_dependency_files)

        # TODO: use different root dir for these temp input files than self._output_root_dir (which is for output files)?
        with tempfile.NamedTemporaryFile(
            mode="wt",
            encoding="utf-8",
            dir=job_work_dir.parent,
            prefix=f"{job_id}_",
            suffix=".in",
        ) as job_specification_file, tempfile.NamedTemporaryFile(
            mode="wt",
            encoding="utf-8",
            dir=job_work_dir.parent,
            prefix=f"{job_id}_",
            suffix=".properties",
        ) as temp_properties_file:
            job_specification_file.write(job_specification_json)
            job_specification_file.flush()

            self._write_sensitive_values(temp_properties_file, vault_token=vault_token)
            temp_properties_file.flush()

            job_name = "openEO batch_{title}_{j}_user {u}".format(title=job_title, j=job_id, u=user_id)
            
            # Calculate docker mounts including user-specific mounts
            docker_mounts = get_backend_config().batch_docker_mounts
            if user_id in get_backend_config().batch_user_docker_mounts:
                docker_mounts = docker_mounts + "," + ",".join(get_backend_config().batch_user_docker_mounts[user_id])
            
            # Determine principal and keytab values - both must be present or both use defaults
            if self._principal is not None and self._key_tab is not None:
                principal_value = self._principal
                keytab_value = self._key_tab
            else:
                principal_value = "no_principal"
                keytab_value = "no_keytab"
            
            backend_config = get_backend_config()
            
            if not backend_config.batch_spark_eventlog_dir:
                raise InternalException("batch_spark_eventlog_dir must be configured in backend config")
            if not backend_config.batch_spark_history_fs_logdirectory:
                raise InternalException("batch_spark_history_fs_logdirectory must be configured in backend config")
            if not backend_config.batch_spark_yarn_historyserver_address:
                raise InternalException("batch_spark_yarn_historyserver_address must be configured in backend config")
            
            # Create structured arguments using dataclass
            submit_args = BatchJobSubmitArgs(
                script_location=script_location,
                job_name=job_name,
                process_graph_file=job_specification_file.name,
                output_dir=str(job_work_dir),
                output_file_name="out",  # TODO: how support multiple output files?
                metadata_file_name=JOB_METADATA_FILENAME,
                principal=principal_value,
                key_tab=keytab_value,
                proxy_user=self._verify_proxy_user(proxy_user or user_id),
                api_version=api_version if api_version else "0.4.0",
                driver_memory=options.driver_memory,
                executor_memory=options.executor_memory,
                executor_memory_overhead=options.executor_memory_overhead,
                driver_cores=str(options.driver_cores),
                executor_cores=str(options.executor_cores),
                driver_memory_overhead=options.driver_memory_overhead,
                queue=queue,
                profile=profile,
                dependencies=self.serialize_dependencies(job_info=job_info),
                py_files=self.get_submit_py_files(log=log) + extra_py_files,
                max_executors=str(options.max_executors),
                user_id=user_id,
                batch_job_id=job_id,
                max_soft_errors_ratio=options.soft_errors_arg(),
                task_cpus=str(options.task_cpus),
                sentinel_hub_client_alias=sentinel_hub_client_alias,
                properties_file=temp_properties_file.name,
                udf_dependency_archives=",".join(options.udf_dependency_archives),
                log_level=options.log_level,
                openeo_backend_config=os.environ.get(ConfigGetter.OPENEO_BACKEND_CONFIG, ""),
                udf_python_dependencies_folder_path=str(job_work_dir / UDF_PYTHON_DEPENDENCIES_FOLDER_NAME),
                ejr_api=backend_config.ejr_api or "",
                ejr_backend_id=backend_config.ejr_backend_id,
                ejr_oidc_client_credentials=os.environ.get("OPENEO_EJR_OIDC_CLIENT_CREDENTIALS", ""),
                docker_mounts=docker_mounts,
                udf_python_dependencies_archive_path=str(job_work_dir / UDF_PYTHON_DEPENDENCIES_ARCHIVE_NAME),
                propagatable_web_app_driver_envars=os.environ.get("OPENEO_PROPAGATABLE_WEB_APP_DRIVER_ENVARS", ""),
                python_max_memory=str(byte_string_as(options.python_memory)) if options.python_memory else "-1",
                spark_eventlog_dir=backend_config.batch_spark_eventlog_dir,
                spark_history_fs_logdirectory=backend_config.batch_spark_history_fs_logdirectory,
                spark_yarn_historyserver_address=backend_config.batch_spark_yarn_historyserver_address,
                yarn_container_runtime_docker_client_config=backend_config.batch_yarn_container_runtime_docker_client_config,
            )
            args = submit_args.to_args_list()

            # TODO: this positional `args` handling is getting out of hand, leverage _write_sensitive_values?

            try:
                log.info(f"Submitting job with command {args!r}")
                d = dict(**os.environ)
                d["YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"] = image_name
                if options.openeo_jar_path is not None:
                    d["OPENEO_GEOTRELLIS_JAR"] = options.openeo_jar_path
                script_output = subprocess.check_output(args, stderr=subprocess.STDOUT, universal_newlines=True, env=d)
                log.info(f"Submitted job, output was: {script_output}")
            except CalledProcessError as e:
                log.error(f"Submitting job failed, output was: {e.stdout}", exc_info=True)
                raise InternalException(message=f"Failed to start batch job (YARN submit failure).")

        try:
            application_id = self._extract_application_id(script_output)
            log.info("mapped job_id %s to application ID %s" % (job_id, application_id))
            return application_id

        except _BatchJobError:
            traceback.print_exc(file=sys.stderr)
            # TODO: why reraise as CalledProcessError?
            raise CalledProcessError(1, str(args), output=script_output)


class _BatchJobError(Exception):
    pass
