import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from subprocess import CalledProcessError
from typing import Union, Optional

import pkg_resources

from openeo.util import deep_get, ensure_dir
from openeo_driver.config.load import ConfigGetter
from openeo_driver.constants import JOB_STATUS
from openeo_driver.errors import InternalException, OpenEOApiException
from openeogeotrellis import sentinel_hub
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.job_options import JobOptions
from openeogeotrellis.udf import UDF_PYTHON_DEPENDENCIES_FOLDER_NAME, UDF_PYTHON_DEPENDENCIES_ARCHIVE_NAME
from openeogeotrellis.utils import add_permissions


JOB_METADATA_FILENAME = "job_metadata.json"

class YARNBatchJobRunner():

    def __init__(self, principal: str = None, key_tab: str = None):
        self._principal = principal
        self._key_tab = key_tab
        self._default_sentinel_hub_client_id = None
        self._default_sentinel_hub_client_secret = None

    def set_default_sentinel_hub_credentials(self, client_id: str, client_secret: str):
        self._default_sentinel_hub_client_id = client_id
        self._default_sentinel_hub_client_secret = client_secret

    def serialize_dependencies(self,job_info, dependencies = None) -> str:
        batch_process_dependencies = [dependency for dependency in
                                      (dependencies or job_info.get('dependencies') or [])
                                      if 'collection_id' in dependency]

        def as_arg_element(dependency: dict) -> dict:
            source_location = (dependency.get('assembled_location')  # cached
                               or dependency.get('results_location')  # not cached
                               or f"s3://{sentinel_hub.OG_BATCH_RESULTS_BUCKET}"
                                  f"/{dependency.get('subfolder') or dependency['batch_request_id']}")  # legacy

            return {
                'source_location': source_location,
                'card4l': dependency.get('card4l', False)
            }

        return json.dumps([as_arg_element(dependency) for dependency in batch_process_dependencies])


    def _write_sensitive_values(self, output_file, vault_token: Optional[str]):
        output_file.write(f"spark.openeo.sentinelhub.client.id.default={self._default_sentinel_hub_client_id}\n")
        output_file.write(f"spark.openeo.sentinelhub.client.secret.default={self._default_sentinel_hub_client_secret}\n")

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
    def get_submit_py_files(env: dict = None, cwd: Union[str, Path] = ".",log=None) -> str:
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



    def run_job(self,job_info:dict,job_id:str,job_work_dir, log,user_id ="", api_version="1.0.0",proxy_user:str=None, vault_token: Optional[str] = None):

        job_process_graph = job_info["process"]["process_graph"]
        job_options = job_info.get("job_options") or {}  # can be None
        job_specification_json = json.dumps({"process_graph": job_process_graph, "job_options": job_options})

        job_title = job_info.get('title', '')
        options = JobOptions.from_dict(job_options)

        ensure_dir(job_work_dir)
        # Ensure others can read/write so that the batch job driver and executors can write to it.
        # The intention is that a cronjob will later only allow the webapp driver to read the results.
        add_permissions(job_work_dir, stat.S_IRWXO | stat.S_IWGRP, None, get_backend_config().non_kube_batch_job_results_dir_group)

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
        sentinel_hub_client_alias = deep_get(job_options, 'sentinel-hub', 'client-alias', default="default")

        submit_script = "submit_batch_job_spark3.sh"
        script_location = pkg_resources.resource_filename("openeogeotrellis.deploy", submit_script)

        image_name = job_options.get("image-name", os.environ.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE"))

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

            self._write_sensitive_values(temp_properties_file,
                                         vault_token=vault_token)
            temp_properties_file.flush()

            job_name = "openEO batch_{title}_{j}_user {u}".format(title=job_title, j=job_id, u=user_id)
            args: list[str] = [
                script_location,
                job_name,
                job_specification_file.name,
                str(job_work_dir),
                "out",  # TODO: how support multiple output files?
                JOB_METADATA_FILENAME,
            ]

            if self._principal is not None and self._key_tab is not None:
                args.append(self._principal)
                args.append(self._key_tab)
            else:
                args.append("no_principal")
                args.append("no_keytab")

            args.append(proxy_user or user_id)

            if api_version:
                args.append(api_version)
            else:
                args.append("0.4.0")

            args.append(options.driver_memory)
            args.append(options.executor_memory)
            args.append(options.executor_memory_overhead)
            args.append(str(options.driver_cores))
            args.append(str(options.executor_cores))
            args.append(options.driver_memory_overhead)
            args.append(queue)
            args.append(profile)
            args.append(self.serialize_dependencies(job_info=job_info))
            args.append(self.get_submit_py_files(log=log) + extra_py_files)
            args.append(str(options.max_executors))
            args.append(user_id)
            args.append(job_id)
            args.append(options.soft_errors_arg())
            args.append(str(options.task_cpus))
            args.append(sentinel_hub_client_alias)
            args.append(temp_properties_file.name)
            args.append(",".join(options.udf_dependency_archives))
            args.append(options.log_level)
            args.append(os.environ.get(ConfigGetter.OPENEO_BACKEND_CONFIG, ""))
            args.append(str(job_work_dir / UDF_PYTHON_DEPENDENCIES_FOLDER_NAME))
            args.append(get_backend_config().ejr_api or "")
            args.append(get_backend_config().ejr_backend_id)
            args.append(os.environ.get("OPENEO_EJR_OIDC_CLIENT_CREDENTIALS", ""))

            docker_mounts = get_backend_config().batch_docker_mounts
            if user_id in get_backend_config().batch_user_docker_mounts:
                docker_mounts = docker_mounts + "," + ",".join(get_backend_config().batch_user_docker_mounts[user_id])
            args.append(docker_mounts)

            args.append(str(job_work_dir / UDF_PYTHON_DEPENDENCIES_ARCHIVE_NAME))
            args.append(os.environ.get("OPENEO_PROPAGATABLE_WEB_APP_DRIVER_ENVARS", ""))

            args.append(str(options.python_memory))

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