import contextlib
import importlib
import os
import shutil
import sys
import typing
import urllib
from datetime import datetime
from pathlib import Path
from typing import Union
from unittest import mock

import boto3
import flask
import moto
import moto.server
import pytest
import requests_mock
import time_machine
from _pytest.terminal import TerminalReporter
from openeo_driver.backend import OpenEoBackendImplementation, UserDefinedProcesses
from openeo_driver.jobregistry import ElasticJobRegistry, JobRegistryInterface
from openeo_driver.testing import ApiTester, ephemeral_fileserver, UrllibMocker
from openeo_driver.utils import smart_bool
from openeo_driver.views import build_app

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.testing import gps_config_overrides
from openeogeotrellis.vault import Vault

from .data import TEST_DATA_ROOT, get_test_data_file

# TODO: Explicitly import these fixtures where there are needed.
from .datacube_fixtures import (
    imagecollection_with_two_bands_and_one_date,
    imagecollection_with_two_bands_and_one_date_multiple_values,
    imagecollection_with_two_bands_and_three_dates,
    imagecollection_with_two_bands_and_three_dates_webmerc,
    imagecollection_with_two_bands_spatial_only,
)

_BACKEND_CONFIG_PATH = Path(__file__).parent / "backend_config.py"


pytest_plugins = "pytester"


@pytest.fixture(scope="session")
def backend_config_path() -> Path:
    return _BACKEND_CONFIG_PATH


@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Pytest configuration hook"""
    os.environ["PYTEST_CONFIGURE"] = (os.environ.get("PYTEST_CONFIGURE", "") + ":" + __file__).lstrip(":")

    # Load test GpsBackendConfig by default
    os.environ["OPENEO_BACKEND_CONFIG"] = str(_BACKEND_CONFIG_PATH)

    # TODO #285 we need a better config system, e.g. to avoid monkeypatching `os.environ` here
    os.environ["BATCH_JOBS_ZOOKEEPER_ROOT_PATH"] = "/openeo.test/jobs"
    os.environ["VAULT_ADDR"] = "https://vault.test"
    os.environ["ASYNC_TASKS_KAFKA_BOOTSTRAP_SERVERS"] = "kafka01.test:6668"

    terminal_reporter = config.pluginmanager.get_plugin("terminalreporter")
    _ensure_geopyspark(terminal_reporter)
    if smart_bool(os.environ.get("OPENEO_TESTING_SETUP_SPARK", "yes")):
        _setup_local_spark(terminal_reporter, verbosity=config.getoption("verbose"))


def _ensure_geopyspark(out: TerminalReporter):
    """Make sure GeoPySpark knows where to find Spark (SPARK_HOME) and py4j"""
    try:
        import geopyspark

        out.write_line("[conftest.py] Succeeded to import geopyspark automatically: {p!r}".format(p=geopyspark))
    except KeyError as e:
        # Geopyspark failed to detect Spark home and py4j, let's fix that.
        from pyspark import find_spark_home

        pyspark_home = Path(find_spark_home._find_spark_home())
        out.write_line(
            "[conftest.py] Failed to import geopyspark automatically. "
            "Will set up py4j path using Spark home: {h}".format(h=pyspark_home)
        )
        py4j_zip = next((pyspark_home / "python" / "lib").glob("py4j-*-src.zip"))
        out.write_line("[conftest.py] py4j zip: {z!r}".format(z=py4j_zip))
        sys.path.append(str(py4j_zip))


def is_port_free(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)  # seconds
        return s.connect_ex(("localhost", port)) != 0


def force_stop_spark_context():
    # Restart SparkContext will make sure that the new environment variables are available inside the JVM
    # This is a hacky way to allow debugging in the same process.
    from pyspark import SparkContext

    with SparkContext._lock:
        # Need to shut down before creating a new SparkConf (Before SparkContext is not enough)
        # Like this, the new environment variables are available inside the JVM
        if SparkContext._active_spark_context:
            SparkContext._active_spark_context.stop()
            SparkContext._gateway.shutdown()
            SparkContext._gateway = None
            SparkContext._jvm = None


def _setup_local_spark(out: TerminalReporter, verbosity=0):
    # TODO make a "spark_context" fixture instead of doing this through pytest_configure
    out.write_line("[conftest.py] Setting up local Spark")
    master_str = "local[2]"

    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable

    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    # Make sure geopyspark can find the custom jars (e.g. geotrellis-extension)
    # even if test suite is not run from project root (e.g. "run this test" functionality in an IDE like PyCharm)
    additional_jar_dirs = [
        Path(__file__).parent / "../jars",
    ]

    conf = geopyspark_conf(
        master=master_str,
        appName="OpenEO-GeoPySpark-Driver-Tests",
        additional_jar_dirs=additional_jar_dirs,
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
    for jar_dir in additional_jar_dirs:
        for jar_path in Path(jar_dir).iterdir():
            if jar_path.match("openeo-logging-*.jar"):
                jars.append(str(jar_path))
    extraClassPath = ":".join(jars)
    conf.set("spark.driver.extraClassPath", extraClassPath)
    conf.set("spark.executor.extraClassPath", extraClassPath)

    sparkSubmitLog4jConfigurationFile = Path(__file__).parent.parent / "scripts/batch_job_log4j2.xml"
    with open(sparkSubmitLog4jConfigurationFile, "r") as read_file:
        content = read_file.read()
        sparkSubmitLog4jConfigurationFile = "/tmp/sparkSubmitLog4jConfigurationFile.xml"
        with open(sparkSubmitLog4jConfigurationFile, "w") as write_file:
            # There could be a more elegant way to fill in this variable during testing:
            write_file.write(
                content.replace("${sys:spark.yarn.app.container.log.dir}/", "").replace(
                    "${sys:openeo.logging.threshold}", "DEBUG"
                )
            )
    # got some options from 'sparkDriverJavaOptions'
    sparkDriverJavaOptions = f"-Dlog4j2.configurationFile=file:{sparkSubmitLog4jConfigurationFile}\
    -Dscala.concurrent.context.numThreads=6 \
    -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
    -Dtsservice.layersConfigClass=ProdLayersConfiguration -Dtsservice.sparktasktimeout=600"
    if OPENEO_LOCAL_DEBUGGING:
        for port in range(5005, 5009):
            if is_port_free(port):
                # 'agentlib' to allow attaching a Java debugger to running Spark driver
                # IntelliJ IDEA: Run -> Edit Configurations -> Remote JVM Debug uses 5005 by default
                sparkDriverJavaOptions += f" -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:{port}"
                break
    conf.set("spark.driver.extraJavaOptions", sparkDriverJavaOptions)

    sparkExecutorJavaOptions = f"-Dlog4j2.configurationFile=file:{sparkSubmitLog4jConfigurationFile}\
     -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
     -Dscala.concurrent.context.numThreads=8"
    conf.set("spark.executor.extraJavaOptions", sparkExecutorJavaOptions)

    out.write_line("[conftest.py] SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)
    context.setLogLevel("DEBUG")
    out.write_line(
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
        print("Spark UI: " + str(context.uiWebUrl))

    out.write_line("[conftest.py] Validating the Spark context")
    dummy = context._jvm.org.openeo.geotrellis.OpenEOProcesses()
    answer = context.parallelize([9, 10, 11, 12]).sum()
    out.write_line("[conftest.py] " + repr((answer, dummy)))

    return context


@pytest.fixture(params=["1.0.0"])
def api_version(request):
    return request.param


# TODO: Deduplicate code with openeo-python-client
class _Sleeper:
    def __init__(self):
        self.history = []

    @contextlib.contextmanager
    def patch(self, time_machine: time_machine.TimeMachineFixture) -> typing.Iterator["_Sleeper"]:
        def sleep(seconds):
            # Note: this requires that `time_machine.move_to()` has been called before
            # also see https://github.com/adamchainz/time-machine/issues/247
            time_machine.coordinates.shift(seconds)
            self.history.append(seconds)

        with mock.patch("time.sleep", new=sleep):
            yield self

    def did_sleep(self) -> bool:
        return len(self.history) > 0


@pytest.fixture
def fast_sleep(time_machine) -> typing.Iterator[_Sleeper]:
    """
    Fixture using `time_machine` to make `sleep` instant and update the current time.
    """
    now = datetime.now().isoformat()
    time_machine.move_to(now)
    with _Sleeper().patch(time_machine=time_machine) as sleeper:
        yield sleeper


@pytest.fixture
def udf_noop():
    file_name = get_test_data_file("udf_noop.py")
    with open(file_name, "r")  as f:
        udf_code = f.read()

    noop_udf_callback = {
        "udf_process": {
            "arguments": {"data": {"from_parameter": "data"}, "udf": udf_code},
            "process_id": "run_udf",
            "result": True,
        },
    }
    return noop_udf_callback


@pytest.fixture
def udf_noop_jep(udf_noop):
    udf_noop["udf_process"]["arguments"]["runtime"] = "Python-Jep"
    return udf_noop


@pytest.fixture
def batch_job_output_root(tmp_path) -> Path:
    # TODO: can we avoid using/initializing tmp_path when we won't need it (or is it low overhead anyway?)
    batch_job_output_root = tmp_path / "jobs"
    batch_job_output_root.mkdir(parents=True)
    return batch_job_output_root


@pytest.fixture
def job_registry() -> InMemoryJobRegistry:
    return InMemoryJobRegistry()


@pytest.fixture
def backend_implementation(batch_job_output_root, job_registry) -> "GeoPySparkBackendImplementation":
    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    backend = GeoPySparkBackendImplementation(
        batch_job_output_root=batch_job_output_root,
        elastic_job_registry=job_registry,
    )
    return backend


@pytest.fixture
def flask_app(backend_implementation) -> flask.Flask:
    app = build_app(
        backend_implementation=backend_implementation,
        # error_handling=False,
    )
    app.config["TESTING"] = True
    app.config["SERVER_NAME"] = "oeo.net"
    return app


@pytest.fixture
def client(flask_app):
    return flask_app.test_client()


@pytest.fixture
def user_defined_process_registry(backend_implementation: OpenEoBackendImplementation) -> UserDefinedProcesses:
    return backend_implementation.user_defined_processes


@pytest.fixture
def api(api_version, client) -> ApiTester:
    return ApiTester(api_version=api_version, client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api100(client) -> ApiTester:
    return ApiTester(api_version="1.0.0", client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def api110(client) -> ApiTester:
    return ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)


@pytest.fixture
def vault() -> Vault:
    return Vault("http://example.org")


@pytest.fixture
def urllib_mock() -> UrllibMocker:
    with UrllibMocker().patch() as mocker:
        yield mocker


class UrllibAndRequestMocker:
    def __init__(self, urllib_mock, requests_mock):
        self.urllib_mock = urllib_mock
        self.requests_mock = requests_mock

    def get(self, href, data):
        code = 200
        self.urllib_mock.get(href, data, code)
        if isinstance(data, str):
            data = data.encode("utf-8")
        self.requests_mock.get(href, content=data)


@pytest.fixture
def urllib_and_request_mock(urllib_mock, requests_mock) -> UrllibAndRequestMocker:
    yield UrllibAndRequestMocker(urllib_mock, requests_mock)


TEST_AWS_REGION_NAME = "eu-central-1"


@pytest.fixture(scope="function")
def aws_credentials(monkeypatch):
    """Mocked AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_AWS_REGION_NAME)
    monkeypatch.setenv("AWS_REGION", TEST_AWS_REGION_NAME)


@pytest.fixture(scope="function")
def mock_s3_resource(aws_credentials):
    with moto.mock_aws():
        yield boto3.resource("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope="function")
def mock_s3_client(aws_credentials):
    with moto.mock_aws():
        yield boto3.client("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope="function")
def mock_s3_bucket(mock_s3_resource, monkeypatch):
    bucket_name = "openeo-fake-bucketname"
    monkeypatch.setenv("SWIFT_BUCKET", bucket_name)

    with gps_config_overrides(s3_bucket_name=bucket_name):
        assert get_backend_config().s3_bucket_name == bucket_name

        bucket = mock_s3_resource.Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION_NAME})
        yield bucket


@pytest.fixture
def moto_server(monkeypatch) -> str:
    """
    Fixture to run Moto in server mode,
    so that subprocesses also can access mocked services
    (when pointed to the correct endpoint URL).
    """
    server = moto.server.ThreadedMotoServer(
        # Automatically find an open port
        port=0,
    )
    server.start()
    endpoint_url = f"http://{server._server.server_address[0]}:{server._server.server_port}"
    monkeypatch.setenv("SWIFT_URL", endpoint_url)
    yield endpoint_url
    server.stop()


@pytest.fixture
def dummy_pypi(tmp_path):
    """
    Fixture for fake PyPI index for testing package installation (without using real PyPI).

    Based on 'PEP 503 – Simple Repository API'

    Also see `unload_dummy_packages` fixture
    (to automatically unload on-the-fly installed dummy packages at the end of a test)
    """
    root = tmp_path / ".package-index"
    root.mkdir(parents=True)
    (root / "index.html").write_text(
        """
        <!DOCTYPE html><html><body>
            <a href="/mehh/">mehh</a>
        </body></html>
        """
    )
    mehh_folder = root / "mehh"
    mehh_folder.mkdir(parents=True)
    shutil.copy(src=get_test_data_file("pip/mehh/dist/mehh-1.2.3-py3-none-any.whl"), dst=mehh_folder)
    (mehh_folder / "index.html").write_text(
        """
        <!DOCTYPE html><html><body>
            <a href="/mehh/mehh-1.2.3-py3-none-any.whl#md5=33c211631375b944c7cb9452074ee3e1">meh-1.2.3-py3-none-any.whl</a>
        </body></html>
        """
    )
    with ephemeral_fileserver(root) as pypi_url:
        yield pypi_url


@pytest.fixture
def unload_dummy_packages():
    """
    Fixture to automatically unload dummy packages at the end of a test,
    to avoid leakage between tests due to import caching mechanisms.

    This fixture should be added to tests that do
    on-the-fly package installation and import of dummy packages like `mehh`

    also see `dummy_pypi` fixture
    """
    packages = ["mehh"]
    yield
    for package in packages:
        if package in sys.modules:
            del sys.modules[package]
    importlib.invalidate_caches()
