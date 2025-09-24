# Development

## Virtual environment setup

As usually recommended in modern Python development,
it is best to use some sort of virtual environment
to sandbox this application and its dependencies.

openEO GeoPySpark depends on
[openeo-python-driver](https://github.com/Open-EO/openeo-python-driver)
in a "bleeding edge" fashion as both projects are developed in tandem.
It is therefor recommended to install both projects from source
in the same virtual environment.
As usual in this kind of development setup,
we will install both projects in "editable" mode (`pip` option `-e`)
so that changes to the source code are immediately reflected in the virtual environment.

In addition, one can also choose to install the
[openeo Python client project](https://github.com/Open-EO/openeo-python-client)
the same way in the virtual environment, if one is planning to have this project
in development scope as well.

### Clone the projects

In your development workspace, clone the projects, e.g. as follows:
```bash
git clone --recursive git@github.com:Open-EO/openeo-geopyspark-driver.git
git clone --recursive git@github.com:Open-EO/openeo-python-driver.git
# Optionally:
git clone git@github.com:Open-EO/openeo-python-client.git
```

### Create the basic virtual environment

We'll create (and acticate) a `venv` in the `openeo-geopyspark-driver` directory.
As noted, you might want to use a different virtual environment tool as desired.

```bash
cd openeo-geopyspark-driver
python -m venv --prompt . venv
source venv/bin/activate
```

### Install the projects in the virtual environment

```bash
# Optional: install the openeo-python-client project from source:
pip install -e ../openeo-python-client

# First install the openeo-python-driver project (with "dev" extra):
pip install -e ../openeo-python-driver[dev]

# Finally install the openeo-geopyspark-driver project itself (with "dev" extra):
pip install -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-openeo/simple
```

## Additional dependencies

The openEO GeoPySpark driver also depends on some JAR files
from [openeo-geotrellis-extensions](https://github.com/Open-EO/openeo-geotrellis-extensions).
these can be fetched with the following script:

```bash
python scripts/get-jars.py
```


## Running locally

You can run the service with:

```bash
export SPARK_HOME=$(find_spark_home.py)
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLASK_DEBUG=1
python openeogeotrellis/deploy/local.py
```


### Running locally with Docker

This will set up the environment (Java/Python with dependencies) and makes it possible to run simple process graphs.
For more information, see: [docker/local_batch_job/](../docker/local_batch_job/README.md)
