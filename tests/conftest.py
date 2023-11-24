import os
from typing import Union

import sys
from pathlib import Path

import boto3
import flask
import pytest
from moto import mock_s3
from _pytest.terminal import TerminalReporter

from openeo_driver.backend import OpenEoBackendImplementation, UserDefinedProcesses
from openeo_driver.jobregistry import ElasticJobRegistry, JobRegistryInterface
from openeo_driver.testing import ApiTester
from openeo_driver.utils import smart_bool
from openeo_driver.views import build_app
from openeogeotrellis.job_registry import InMemoryJobRegistry
from openeogeotrellis.vault import Vault

from .datacube_fixtures import imagecollection_with_two_bands_and_three_dates, \
    imagecollection_with_two_bands_and_one_date, imagecollection_with_two_bands_and_three_dates_webmerc, \
    imagecollection_with_two_bands_spatial_only
from .data import get_test_data_file, TEST_DATA_ROOT

os.environ["OPENEO_CATALOG_FILES"] = str(Path(__file__).parent / "layercatalog.json")


pytest_plugins = "pytester"



@pytest.hookimpl(trylast=True)
def pytest_configure(config):
    """Pytest configuration hook"""
    os.environ['PYTEST_CONFIGURE'] = (os.environ.get('PYTEST_CONFIGURE', '') + ':' + __file__).lstrip(':')

    # Load test GpsBackendConfig by default
    os.environ["OPENEO_BACKEND_CONFIG"] = str(Path(__file__).parent / "backend_config.py")

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
        out.write_line("[conftest.py] Failed to import geopyspark automatically. "
                       "Will set up py4j path using Spark home: {h}".format(h=pyspark_home))
        py4j_zip = next((pyspark_home / 'python' / 'lib').glob('py4j-*-src.zip'))
        out.write_line("[conftest.py] py4j zip: {z!r}".format(z=py4j_zip))
        sys.path.append(str(py4j_zip))


def _setup_local_spark(out: TerminalReporter, verbosity=0):
    # TODO make a "spark_context" fixture instead of doing this through pytest_configure
    out.write_line("[conftest.py] Setting up local Spark")
    master_str = "local[2]"

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable

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

    # Use UTC timezone by default when formatting/parsing dates (e.g. CSV export of timeseries)
    conf.set("spark.sql.session.timeZone", "UTC")

    conf.set("spark.kryoserializer.buffer.max", value="1G")
    conf.set(key='spark.kryo.registrator', value='geotrellis.spark.store.kryo.KryoRegistrator')
    conf.set(
        key="spark.kryo.classesToRegister",
        value="org.openeo.geotrellisaccumulo.SerializableConfiguration,ar.com.hjg.pngj.ImageInfo,ar.com.hjg.pngj.ImageLineInt,geotrellis.raster.RasterRegion$GridBoundsRasterRegion",
    )
    # Only show spark progress bars for high verbosity levels
    conf.set('spark.ui.showConsoleProgress', verbosity >= 3)

    conf.set(key="spark.driver.memory", value="2G")
    conf.set(key="spark.executor.memory", value="2G")
    OPENEO_LOCAL_DEBUGGING = smart_bool(os.environ.get("OPENEO_LOCAL_DEBUGGING", "false"))
    conf.set('spark.ui.enabled', OPENEO_LOCAL_DEBUGGING)

    jars = []
    for jar_dir in additional_jar_dirs:
        for jar_path in Path(jar_dir).iterdir():
            if jar_path.match("openeo-logging-*.jar"):
                jars.append(str(jar_path))
    extraClassPath = ":".join(jars)
    conf.set('spark.driver.extraClassPath', extraClassPath)
    conf.set('spark.executor.extraClassPath', extraClassPath)

    sparkSubmitLog4jConfigurationFile = Path(__file__).parent.parent / "scripts/batch_job_log4j2.xml"
    with open(sparkSubmitLog4jConfigurationFile, 'r') as read_file:
        content = read_file.read()
        sparkSubmitLog4jConfigurationFile = "/tmp/sparkSubmitLog4jConfigurationFile.xml"
        with open(sparkSubmitLog4jConfigurationFile, 'w') as write_file:
            # There could be a more elegant way to fill in this variable during testing:
            write_file.write(content
                             .replace("${sys:spark.yarn.app.container.log.dir}/", "")
                             .replace("${sys:openeo.logging.threshold}", "DEBUG")
                             )

    # 'agentlib' to allow attaching a Java debugger to running Spark driver
    extra_options = f'-Dlog4j2.configurationFile=file:{sparkSubmitLog4jConfigurationFile}'
    if OPENEO_LOCAL_DEBUGGING:
        extra_options += f' -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5009'
    conf.set('spark.driver.extraJavaOptions', extra_options)
    # conf.set('spark.executor.extraJavaOptions', extra_options) # Seems not needed


    out.write_line("[conftest.py] SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)
    context.setLogLevel("DEBUG")
    out.write_line("[conftest.py] JVM info: {d!r}".format(d={
        f: context._jvm.System.getProperty(f)
        for f in [
            "java.version", "java.vendor", "java.home",
            "java.class.version",
            # "java.class.path",
        ]
    }))

    out.write_line("[conftest.py] Validating the Spark context")
    dummy = context._jvm.org.openeo.geotrellis.OpenEOProcesses()
    answer = context.parallelize([9, 10, 11, 12]).sum()
    out.write_line("[conftest.py] " + repr((answer, dummy)))

    return context


@pytest.fixture(params=["1.0.0"])
def api_version(request):
    return request.param


@pytest.fixture
def udf_noop():
    file_name = get_test_data_file("udf_noop.py")
    with open(file_name, "r")  as f:
        udf_code = f.read()

    noop_udf_callback = {
        "udf_process": {
            "arguments": {
                "data": {
                    "from_parameter": "data"
                },
                "udf": udf_code
            },
            "process_id": "run_udf",
            "result": True
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
def backend_implementation(
    request, batch_job_output_root, job_registry
) -> "GeoPySparkBackendImplementation":
    from openeogeotrellis.backend import GeoPySparkBackendImplementation

    backend = GeoPySparkBackendImplementation(
        batch_job_output_root=batch_job_output_root,
        elastic_job_registry=job_registry,
    )

    # TODO: eliminate this `request.instance` stuff, normal fixture usage should suffice
    if request.instance:
        request.instance.backend_implementation = backend
    return backend


@pytest.fixture
def flask_app(backend_implementation) -> flask.Flask:
    app = build_app(
        backend_implementation=backend_implementation,
        # error_handling=False,
    )
    app.config['TESTING'] = True
    app.config['SERVER_NAME'] = 'oeo.net'
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
    with mock_s3():
        yield boto3.resource("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope="function")
def mock_s3_client(aws_credentials):
    with mock_s3():
        yield boto3.s3_client("s3", region_name=TEST_AWS_REGION_NAME)


@pytest.fixture(scope="function")
def mock_s3_bucket(mock_s3_resource, monkeypatch):
    # TODO: bucket_name: there could be a mismatch with ConfigParams().s3_bucket_name if any ConfigParams instances were created earlier in the test setup.
    bucket_name = "openeo-fake-bucketname"
    monkeypatch.setenv("SWIFT_BUCKET", bucket_name)
    from openeogeotrellis.configparams import ConfigParams

    assert ConfigParams().s3_bucket_name == bucket_name

    bucket = mock_s3_resource.Bucket(bucket_name)
    bucket.create(CreateBucketConfiguration={"LocationConstraint": TEST_AWS_REGION_NAME})
    yield bucket
