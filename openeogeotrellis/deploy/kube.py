"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import base64
import json
import logging
import os
import re
import textwrap

from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME

from openeo_driver.processes import ProcessArgs
from openeo_driver.ProcessGraphDeserializer import (
    ENV_DRY_RUN_TRACER,
    ProcessSpec,
    non_standard_process,
)
from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import (
    LOG_HANDLER_STDERR_JSON,
    FlaskRequestCorrelationIdLogging,
    get_logging_config,
    setup_logging,
)
from openeo_driver.utils import EvalEnv
from openeo_driver.views import build_app

from openeogeotrellis import deploy
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.deploy import get_socket
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.util.runtime import get_job_id, get_request_id

log = logging.getLogger(__name__)


def main():
    # By default, use JSON logging to stderr,
    # but allow overriding this with an environment variable
    root_handler = os.environ.get("OPENEO_LOGGING_ROOT_HANDLER", LOG_HANDLER_STDERR_JSON)

    setup_logging(
        get_logging_config(
            root_handlers=[root_handler],
            loggers={
                "openeo": {"level": "DEBUG"},
                "openeo_driver": {"level": "DEBUG"},
                "openeogeotrellis": {"level": "DEBUG"},
                "flask": {"level": "DEBUG"},
                "werkzeug": {"level": "DEBUG"},
                "gunicorn": {"level": "INFO"},
                "kazoo": {"level": "WARN"},
            },
        )
    )

    from pyspark import SparkContext

    log.info("starting spark context")
    SparkContext.getOrCreate()

    def setup_batch_jobs():
        if get_backend_config().use_zk_job_registry:
            # TODO #236/#498/#632 Phase out ZkJobRegistry?
            with ZkJobRegistry() as job_registry:
                job_registry.ensure_paths()

    def on_started():
        app.logger.setLevel("DEBUG")
        deploy.load_custom_processes()
        setup_batch_jobs()

    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    backend_implementation = GeoPySparkBackendImplementation(use_job_registry=bool(get_backend_config().ejr_api))
    app = build_app(backend_implementation=backend_implementation)

    # https://github.com/Open-EO/openeo-python-driver/issues/242
    # A more generic deployment specific override system does not yet exist, so do it here.
    processes = backend_implementation.processing.get_process_registry("1.2.0")
    backscatter_spec = processes.get_spec("sar_backscatter")
    backscatter_spec["experimental"] = False
    backscatter_spec["description"] = (
        backscatter_spec["description"]
        + """
    \n\n ## Backend notes \n\n The implementation in this backend is based on Orfeo Toolbox.
    """
    )
    parameters = {p["name"]: p for p in backscatter_spec["parameters"]}
    parameters["coefficient"]["default"] = "sigma0-ellipsoid"
    parameters["coefficient"][
        "description"
    ] = "Select the radiometric correction coefficient. The following options are available:\n\n* `sigma0-ellipsoid`: ground area computed with ellipsoid earth model\n"
    parameters["coefficient"]["schema"] = [
        {"type": "string", "enum": ["sigma0-ellipsoid"]},
        {"title": "Non-normalized backscatter", "type": "null"},
    ]
    backscatter_spec["links"].append(
        {
            "rel": "about",
            "href": "https://www.orfeo-toolbox.org/CookBook/Applications/app_SARCalibration.html",
            "title": "Orfeo toolbox backscatter processor.",
        }
    )

    host = os.environ.get('SPARK_LOCAL_IP', None)
    if host is None:
        host, _ = get_socket()
    port = os.environ.get('KUBE_OPENEO_API_PORT', 50001)

    run_gunicorn(
        app,
        threads=30,
        host=host,
        port=port,
        on_started=on_started
    )


@non_standard_process(
    ProcessSpec(id="_cwl_demo", description="Proof-of-concept process to run CWL based processing.")
    .param(name="name", description="Name to greet.", schema={"type": "string"}, required=False)
    .returns(description="data", schema={"type": "string"})
)
def _cwl_demo(args: ProcessArgs, env: EvalEnv):
    """Proof of concept openEO process to run CWL based processing"""
    name = args.get_optional(
        "name",
        default="World",
        validator=ProcessArgs.validator_generic(
            # TODO: helper to create regex based validator
            lambda n: bool(re.fullmatch("^[a-zA-Z]+$", n)),
            error_message="Must be a simple name, but got {actual!r}.",
        ),
    )

    if env.get(ENV_DRY_RUN_TRACER):
        return "dummy"

    # TODO: move this imports to top-level?
    import kubernetes.config

    from openeogeotrellis.integrations.calrissian import CalrissianJobLauncher

    # TODO: better place to load this config?
    if os.path.exists(SERVICE_TOKEN_FILENAME):
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config()

    launcher = CalrissianJobLauncher.from_context()

    cwl_content = textwrap.dedent(
        """
        cwlVersion: v1.0
        class: CommandLineTool
        baseCommand: echo
        requirements:
          - class: DockerRequirement
            dockerPull: debian:stretch-slim
        inputs:
          message:
            type: string
            default: "Hello World"
            inputBinding:
              position: 1
        outputs:
          output_file:
            type: File
            outputBinding:
              glob: output.txt
        stdout: output.txt
    """
    )
    correlation_id = get_job_id(default=None) or get_request_id(default=None)
    cwl_arguments = [
        "--message",
        f"Hello {name}, greetings from {correlation_id}.",
    ]

    results = launcher.run_cwl_workflow(
        cwl_content=cwl_content,
        cwl_arguments=cwl_arguments,
        output_paths=["output.txt"],
    )

    return results["output.txt"].read(encoding="utf8")



@non_standard_process(
    ProcessSpec(id="_cwl_insar", description="Proof-of-concept process to run CWL based inSAR.")
    .param(name="spatial_extent", description="Spatial extent.", schema={"type": "dict"}, required=False)
    .param(name="temporal_extent", description="Temporal extent.", schema={"type": "dict"}, required=False)
    .returns(description="the data as a data cube", schema={})
)
def _cwl_insar(args: ProcessArgs, env: EvalEnv):
    """Proof of concept openEO process to run CWL based processing"""
    spatial_extent = args.get_optional(
        "spatial_extent",
        default=None,
    )
    temporal_extent = args.get_optional(
        "temporal_extent",
        default=None,
    )

    if env.get(ENV_DRY_RUN_TRACER):
        return "dummy"

    from openeo_driver import dry_run

    # source_id = dry_run.DataSource.load_disk_data(**kwargs).get_source_id()
    # load_params = _extract_load_parameters(env, source_id=source_id)

    # TODO: move this imports to top-level?
    import kubernetes.config

    from openeogeotrellis.integrations.calrissian import CalrissianJobLauncher

    # TODO: better place to load this config?
    if os.path.exists(SERVICE_TOKEN_FILENAME):
        kubernetes.config.load_incluster_config()
    else:
        kubernetes.config.load_kube_config()

    launcher = CalrissianJobLauncher.from_context()

    cwl_content = textwrap.dedent(
        f"""
        cwlVersion: v1.0
        class: CommandLineTool
        baseCommand: insar.py
        requirements:
          DockerRequirement:
            dockerPull: registry.stag.warsaw.openeo.dataspace.copernicus.eu/rand/openeo_insar:latest
          EnvVarRequirement:
            envDef:
              AWS_ACCESS_KEY_ID: {json.dumps(os.environ.get("AWS_ACCESS_KEY_ID", ""))}
              AWS_SECRET_ACCESS_KEY: {json.dumps(os.environ.get("AWS_SECRET_ACCESS_KEY", ""))}
        inputs:
          input_base64_json:
            type: string
            inputBinding:
              position: 1
        outputs:
          output_file:
            type:
              type: array
              items: File
            outputBinding:
              glob: "*.*"
    """
    )
    # correlation_id = get_job_id(default=None) or get_request_id(default=None)
    input_base64_json = base64.b64encode(json.dumps(args).encode("utf8")).decode("ascii")
    cwl_arguments = ["--input_base64_json", input_base64_json]

    # TODO: Load the results as datacube with load_stac.
    results = launcher.run_cwl_workflow(
        cwl_content=cwl_content,
        cwl_arguments=cwl_arguments,
        output_paths=["output.txt"],
    )

    return results["output.txt"].read(encoding="utf8")


if __name__ == '__main__':
    main()
