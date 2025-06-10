#!/usr/bin/env python3
"""
Script to start a local server. This script can serve as the entry-point for doing spark-submit.
"""

import logging
import os
import socket
import subprocess
import sys
from glob import glob
from pathlib import Path
from typing import Optional

import openeo_driver.config.load
from openeo_driver.server import run_gunicorn
from openeo_driver.util.logging import get_logging_config, setup_logging, show_log_level, LOG_HANDLER_STDERR_JSON
from openeo_driver.utils import smart_bool
from openeo_driver.views import build_app
from openeogeotrellis.config import get_backend_config

_log = logging.getLogger(__name__)


def is_port_free(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)  # seconds
        return s.connect_ex(("localhost", port)) != 0


def setup_local_spark(log_dir: Path = Path.cwd(), verbosity=0):
    # TODO: make this more reusable (e.g. also see `_setup_local_spark` in tests/conftest.py)
    from pyspark import SparkContext, find_spark_home

    spark_python = os.path.join(find_spark_home._find_spark_home(), "python")
    logging.info(f"spark_python: {spark_python}")
    py4j = glob(os.path.join(spark_python, "lib", "py4j-*.zip"))[0]
    sys.path[:0] = [spark_python, py4j]
    _log.debug("sys.path: {p!r}".format(p=sys.path))
    try:
        # TODO: Find better way to support local_batch_job and @non_standard_process at the same time
        sys.path.append(str(Path(__file__).parent))
        import kube
    except ImportError as e:
        _log.warning(
            "Failed to import kube. Some kubernetes specific processes might not get attached (CWL). error: " + str(e)
        )
    master_str = "local[*]"

    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable

    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    conf = geopyspark_conf(
        master=master_str,
        appName="openeo-geopyspark-driver",
        additional_jar_dirs=[],  # passed with GEOPYSPARK_JARS_PATH
    )

    spark_jars = conf.get("spark.jars").split(",")
    # geotrellis-extensions needs to be loaded first to avoid "java.lang.NoClassDefFoundError: shapeless/lazily$"
    spark_jars.sort(key=lambda x: "geotrellis-extensions" not in x)
    conf.set(key="spark.jars", value=",".join(spark_jars))

    # Use UTC timezone by default when formatting/parsing dates (e.g. CSV export of timeseries)
    conf.set("spark.sql.session.timeZone", "UTC")

    conf.set("spark.kryoserializer.buffer.max", value="1G")
    conf.set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
    conf.set(
        key="spark.kryo.classesToRegister",
        value="ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion",
    )
    # Only show spark progress bars for high verbosity levels
    conf.set("spark.ui.showConsoleProgress", verbosity >= 3)

    conf.set(key="spark.executor.pyspark.memory", value="3G")
    conf.set(key="spark.driver.memory", value="2G")
    conf.set(key="spark.executor.memory", value="2G")
    OPENEO_LOCAL_DEBUGGING = smart_bool(os.environ.get("OPENEO_LOCAL_DEBUGGING", "false"))
    conf.set("spark.ui.enabled", OPENEO_LOCAL_DEBUGGING)

    jars = []
    more_jars = [] if "GEOPYSPARK_JARS_PATH" not in os.environ else os.environ["GEOPYSPARK_JARS_PATH"].split(":")
    for jar_dir in more_jars:
        for jar_path in Path(jar_dir).iterdir():
            if jar_path.match("openeo-logging-*.jar"):
                jars.append(str(jar_path))
    extraClassPath = ":".join(jars)
    conf.set("spark.driver.extraClassPath", extraClassPath)
    conf.set("spark.executor.extraClassPath", extraClassPath)

    path = "/opt/venv/openeo-geopyspark-driver/batch_job_log4j2.xml"  # TODO: get path from data_files
    if os.path.exists(path):
        sparkSubmitLog4jConfigurationFile = path
    else:
        sparkSubmitLog4jConfigurationFile = Path(__file__).parent.parent.parent / "scripts/batch_job_log4j2.xml"
    logging_threshold = "INFO"
    # got some options from 'sparkDriverJavaOptions'
    sparkDriverJavaOptions = f"-Dlog4j2.configurationFile=file:{sparkSubmitLog4jConfigurationFile} \
        -Dspark.yarn.app.container.log.dir={log_dir} \
        -Dopeneo.logging.threshold={logging_threshold} \
        -Dscala.concurrent.context.numThreads=6 \
        -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
        -Dtsservice.layersConfigClass=ProdLayersConfiguration -Dtsservice.sparktasktimeout=600 "
    sparkDriverJavaOptions += " -Dgeotrellis.jts.precision.type=fixed -Dgeotrellis.jts.simplification.scale=1e10"
    if OPENEO_LOCAL_DEBUGGING:
        for port in range(5005, 5009):
            if is_port_free(port):
                # 'agentlib' to allow attaching a Java debugger to running Spark driver
                # IntelliJ IDEA: Run -> Edit Configurations -> Remote JVM Debug uses 5005 by default
                sparkDriverJavaOptions += f" -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:{port}"
                break
    conf.set("spark.driver.extraJavaOptions", sparkDriverJavaOptions)

    sparkExecutorJavaOptions = f"-Dlog4j2.configurationFile=file:{sparkSubmitLog4jConfigurationFile}\
        -Dopeneo.logging.threshold={logging_threshold} \
        -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
        -Dscala.concurrent.context.numThreads=8"
    conf.set("spark.executor.extraJavaOptions", sparkExecutorJavaOptions)

    _log.info("[conftest.py] SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)
    context.setLogLevel(logging_threshold)
    _log.info(
        "[conftest.py] JVM info: {d!r}".format(
            d={
                f: context._jvm.System.getProperty(f)
                for f in [
                    "java.version",
                    "java.vendor",
                    "java.home",
                    "java.class.version",
                    # "java.class.path",
                ]
            }
        )
    )

    if OPENEO_LOCAL_DEBUGGING:
        # TODO: Activate default logging for this message
        print("Spark web UI: " + str(context.uiWebUrl))

    if OPENEO_LOCAL_DEBUGGING:
        _log.info("[conftest.py] Validating the Spark context")
        dummy = context._jvm.org.openeo.geotrellis.OpenEOProcesses()
        answer = context.parallelize([9, 10, 11, 12]).sum()
        _log.info("[conftest.py] " + repr((answer, dummy)))

    return context


def on_started() -> None:
    show_log_level(logging.getLogger("gunicorn.error"))
    show_log_level(logging.getLogger("flask"))
    show_log_level(logging.getLogger("werkzeug"))


def get_minikube_ip() -> Optional[str]:
    try:
        # throw exception if minikube is not configured
        minikube_ip = subprocess.run(["minikube", "ip"], stdout=subprocess.PIPE, text=True).stdout.strip()
        socket.inet_aton(minikube_ip)  # throw exception if not a valid IP address
        return minikube_ip
    except Exception as e:
        _log.warning(f"Failed to get minikube IP: {e}")
        return None

def setup_environment(log_dir: Path = Path.cwd()):
    repository_root = Path(__file__).parent.parent.parent
    if os.path.exists(repository_root / "jars"):
        previous = (":" + os.environ["GEOPYSPARK_JARS_PATH"]) if "GEOPYSPARK_JARS_PATH" in os.environ else ""
        os.environ["GEOPYSPARK_JARS_PATH"] = str(repository_root / "jars") + previous

    os.environ["PYTEST_CONFIGURE"] = ""  # to enable is_ci_context
    os.environ["FLASK_DEBUG"] = "1"

    _log.info(repr({"pid": os.getpid(), "interpreter": sys.executable, "version": sys.version, "argv": sys.argv}))

    setup_local_spark(log_dir=log_dir)

    # Configure access to local minio to ease testing with calrissian: (Documented here: docs/calrissian-cwl.md)
    minikube_ip = get_minikube_ip()
    if minikube_ip:
        os.environ.setdefault("SWIFT_URL", f"http://{minikube_ip}:30000/")
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
        os.environ.setdefault("SWIFT_ACCESS_KEY_ID", "minioadmin")
        os.environ.setdefault("SWIFT_SECRET_ACCESS_KEY", "minioadmin")

    os.environ.setdefault(
        openeo_driver.config.load.ConfigGetter.OPENEO_BACKEND_CONFIG,
        str(Path(__file__).parent / "local_backend_config.py"),
    )


def main():
    root_handlers = [LOG_HANDLER_STDERR_JSON]
    if smart_bool(os.environ.get("OPENEO_DRIVER_SIMPLE_LOGGING")):
        root_handlers = None

    setup_logging(
        get_logging_config(
            root_handlers=root_handlers,
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

    setup_environment()

    # Note: local import is necessary because `openeogeotrellis.backend` requires `SPARK_HOME` env var
    # which we want to set up just in time
    from openeogeotrellis.backend import GeoPySparkBackendImplementation
    from openeogeotrellis.job_registry import InMemoryJobRegistry

    backend_implementation = GeoPySparkBackendImplementation(
        use_zookeeper=False,
        use_job_registry=bool(get_backend_config().ejr_api),
        elastic_job_registry=InMemoryJobRegistry(),
    )
    app = build_app(backend_implementation=backend_implementation)

    show_log_level(logging.getLogger("openeo"))
    show_log_level(logging.getLogger("openeo_driver"))
    show_log_level(logging.getLogger("openeogeotrellis"))
    show_log_level(app.logger)

    host = os.environ.get("OPENEO_DEV_GUNICORN_HOST", "127.0.0.1")

    run_gunicorn(app, threads=4, host=host, port=8080, on_started=on_started)


if __name__ == "__main__":
    main()
