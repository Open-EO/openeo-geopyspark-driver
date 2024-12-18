"""
Script to start a production server on Kubernetes. This script can serve as the mainApplicationFile for the SparkApplication custom resource of the spark-operator
"""

import logging
import os
import re
import textwrap

from openeo_driver.processes import ProcessArgs
from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import (
    get_logging_config,
    setup_logging,
    LOG_HANDLER_STDERR_JSON,
    FlaskRequestCorrelationIdLogging,
)
from openeo_driver.ProcessGraphDeserializer import non_standard_process, ProcessSpec, ENV_DRY_RUN_TRACER
from openeo_driver.utils import EvalEnv
from openeo_driver.views import build_app
from openeogeotrellis import deploy
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.deploy import get_socket
from openeogeotrellis.job_registry import ZkJobRegistry
from openeogeotrellis.utils import get_s3_file_contents, s3_client

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
    .param(name="name", description="Name to greet", schema={"type": "string"}, required=False)
    .returns(description="data", schema={})
)
def _cwl_demo(args: ProcessArgs, env: EvalEnv):
    """Proof of concept openEO process to run CWL based processing"""
    name = args.get_optional(
        "name",
        default="World",
        validator=ProcessArgs.validator_generic(
            lambda n: bool(re.fullmatch("^[a-zA-Z]+$", n)), error_message="Must be a simple name, but got {actual!r}."
        ),
    )

    log = logging.getLogger("openeogeotrellis.deploy.kube._cwl_demo")

    if env.get(ENV_DRY_RUN_TRACER):
        return name

    # TODO: move this imports to top-level?
    import kubernetes.config
    from openeogeotrellis.integrations.calrissian import CalrissianJobLauncher

    request_id = FlaskRequestCorrelationIdLogging.get_request_id()
    name_base = request_id[:20]
    namespace = "calrissian-demo-project"

    kubernetes.config.load_incluster_config()
    launcher = CalrissianJobLauncher(namespace=namespace, name_base=name_base)

    # Input staging
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
    input_staging_manifest, cwl_path = launcher.create_input_staging_job_manifest(cwl_content=cwl_content)
    input_staging_job = launcher.launch_job_and_wait(manifest=input_staging_manifest)

    # CWL job
    cwl_manifest, relative_output_dir = launcher.create_cwl_job_manifest(
        cwl_path=cwl_path,
        cwl_arguments=[
            "--message",
            f"Hello {name}, greetings from {request_id}.",
        ],
    )
    cwl_job = launcher.launch_job_and_wait(manifest=cwl_manifest)

    output_volume_name = launcher.get_output_volume_name()
    s3_instance = s3_client()
    # TODO: get S3 bucket name from config?
    s3_bucket = "calrissian"
    # TODO: this must correspond with the CWL output definition
    s3_key = f"{output_volume_name}/{relative_output_dir.strip('/')}/output.txt"
    log.info(f"Getting CWL output from S3: {s3_bucket=}, {s3_key=}")
    s3_file_object = s3_instance.get_object(Bucket=s3_bucket, Key=s3_key)
    body = s3_file_object["Body"]
    content = body.read().decode("utf8")
    return content



if __name__ == '__main__':
    main()
