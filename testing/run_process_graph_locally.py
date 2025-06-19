import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from openeo.util import ensure_dir
from openeo.internal.graph_building import as_flat_graph

_BACKEND_CONFIG_PATH = Path(__file__).parent / "backend_config.py"

def setup_environment(classpath: str, debug: bool):
    """Pytest configuration hook"""
    os.environ["PYTEST_CONFIGURE"] = (os.environ.get("PYTEST_CONFIGURE", "") + ":" + __file__).lstrip(":")

    # Load test GpsBackendConfig by default
    os.environ["OPENEO_BACKEND_CONFIG"] = str(_BACKEND_CONFIG_PATH)

    # TODO #285 we need a better config system, e.g. to avoid monkeypatching `os.environ` here
    os.environ["BATCH_JOBS_ZOOKEEPER_ROOT_PATH"] = "/openeo.test/jobs"
    os.environ["VAULT_ADDR"] = "https://vault.test"
    os.environ["ASYNC_TASKS_KAFKA_BOOTSTRAP_SERVERS"] = "kafka01.test:6668"

    _ensure_geopyspark()
    _setup_local_spark(classpath, debug)


def _ensure_geopyspark():
    """Make sure GeoPySpark knows where to find Spark (SPARK_HOME) and py4j"""
    try:
        import geopyspark

        print("Succeeded to import geopyspark automatically: {p!r}".format(p=geopyspark))
    except KeyError as e:
        # Geopyspark failed to detect Spark home and py4j, let's fix that.
        from pyspark import find_spark_home

        pyspark_home = Path(find_spark_home._find_spark_home())
        print(
            "Failed to import geopyspark automatically. "
            "Will set up py4j path using Spark home: {h}".format(h=pyspark_home)
        )
        py4j_zip = next((pyspark_home / "python" / "lib").glob("py4j-*-src.zip"))
        print("py4j zip: {z!r}".format(z=py4j_zip))
        sys.path.append(str(py4j_zip))


def is_port_free(port: int) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)  # seconds
        return s.connect_ex(("localhost", port)) != 0


def _setup_local_spark(classpath: str, debug: bool):
    print("Setting up local Spark")
    master_str = "local[2]"

    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable

    from geopyspark import geopyspark_conf
    from pyspark import SparkContext

    conf = geopyspark_conf(
        master=master_str,
        appName="OpenEO-GeoPySpark-Driver-Tests",
        additional_jar_dirs=[],
    )

    spark_jars = conf.get("spark.jars").split(",")
    logging.error(f"SPARK JARS {spark_jars}")
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

    conf.set(key="spark.executor.pyspark.memory", value="3G")
    conf.set(key="spark.driver.memory", value="2G")
    conf.set(key="spark.executor.memory", value="2G")
    conf.set("spark.ui.enabled", debug)

    conf.set("spark.driver.extraClassPath", classpath)
    conf.set("spark.executor.extraClassPath", classpath)

    spark_submit_log4j_configuration_file = Path(__file__).parent.parent / "scripts/batch_job_log4j2.xml"
    with open(spark_submit_log4j_configuration_file, "r") as read_file:
        content = read_file.read()
        spark_submit_log4j_configuration_file = "/tmp/sparkSubmitLog4jConfigurationFile.xml"
        with open(spark_submit_log4j_configuration_file, "w") as write_file:
            # There could be a more elegant way to fill in this variable during testing:
            write_file.write(
                content.replace("${sys:spark.yarn.app.container.log.dir}/", "").replace(
                    "${sys:openeo.logging.threshold}", "DEBUG"
                )
            )
    # got some options from 'sparkDriverJavaOptions'
    spark_driver_java_options = f"-Dscala.concurrent.context.numThreads=6 \
    -Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
    -Dtsservice.layersConfigClass=ProdLayersConfiguration -Dtsservice.sparktasktimeout=600"
    if debug:
        for port in range(5005, 5009):
            if is_port_free(port):
                # 'agentlib' to allow attaching a Java debugger to running Spark driver
                # IntelliJ IDEA: Run -> Edit Configurations -> Remote JVM Debug uses 5005 by default
                spark_driver_java_options += f" -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:{port}"
                break
    conf.set("spark.driver.extraJavaOptions", spark_driver_java_options)

    spark_executor_java_options = f"-Dsoftware.amazon.awssdk.http.service.impl=software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService\
     -Dscala.concurrent.context.numThreads=8"
    conf.set("spark.executor.extraJavaOptions", spark_executor_java_options)

    print("SparkContext.getOrCreate with {c!r}".format(c=conf.getAll()))
    context = SparkContext.getOrCreate(conf)
    if debug:
        context.setLogLevel("DEBUG")
    return context


def run_graph_locally(process_graph, output_dir, classpath: str, debug: bool):
    files_orig = set(output_dir.rglob("*"))
    process_start = datetime.now()
    output_dir = ensure_dir(output_dir)
    setup_environment(classpath, debug)
    # Can only import after setup_environment:
    from openeogeotrellis.backend import JOB_METADATA_FILENAME
    from openeogeotrellis.deploy.batch_job import run_job

    process_graph = as_flat_graph(process_graph)
    if "process_graph" not in process_graph:
        process_graph = {"process_graph": process_graph}
    run_job(
        process_graph,
        output_file=output_dir / "out",  # just like in backend.py
        metadata_file=output_dir / JOB_METADATA_FILENAME,
        api_version="2.0.0",
        job_dir=output_dir,
        dependencies=[],
        user_id="run_graph_locally",
    )
    # Set the permissions so any user can read and delete the files:
    # For when running inside a docker container.
    files_now = set(output_dir.rglob("*"))
    files_new = filter(lambda f: f not in files_orig or f.stat().st_mtime > process_start.timestamp(), files_now)
    for file in files_new:
        if file.is_file():
            os.chmod(file, 0o666)


def main():
    """
    for setup.py entry_points
    """
    if len(sys.argv) < 4:
        print("Usage: run_process_graph_locally.py path/to/process_graph.json path/to/output/ <colon_separated_classpath>")
        sys.exit(1)
    process_graph_path = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    classpath = sys.argv[3]
    debug = len(sys.argv) > 4 and sys.argv[4] == 'DEBUG'
    for f in classpath.split(':'):
        if f.endswith(".jar"):
            if not os.path.exists(f):
                logging.error(f"Jar is missing: {f}")
        else:
            if not os.path.isdir(f):
                logging.error(f"Classpath folder is missing: {f}")
    run_graph_locally(process_graph_path, output_dir, classpath, debug)


if __name__ == "__main__":
    main()
