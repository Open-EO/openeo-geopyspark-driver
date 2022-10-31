## OpenEO Geopyspark Driver

[![Status](https://img.shields.io/badge/Status-proof--of--concept-yellow.svg)]()

Python version: at least 3.8

This driver implements the GeoPySpark/Geotrellis specific backend for OpenEO.

It does this by implementing a direct (non-REST) version of the OpenEO client API on top 
of [GeoPySpark](https://github.com/locationtech-labs/geopyspark/). 

A REST service based on Flask translates incoming calls to this local API.

![Technology stack](openeo-geotrellis-techstack.png?raw=true "Technology stack")

### Operating environment dependencies
This backend has been tested with:
- Something that runs Spark: Kubernetes or YARN (Hadoop), standalone or on your laptop
- Accumulo as the tile storage backend for Geotrellis
- Reading GeoTiff files directly from disk or object storage

### Public endpoint
https://openeo.vito.be/openeo/

### Running locally

Set up your (virtual) environment with necessary dependencies:

    # Install Python package and its depdendencies
    pip install .[dev] --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
    
    # Get necessary jars for Geopyspark
    python scripts/get-jars.py

 
For development, you can run the service:

    export SPARK_HOME=$(find_spark_home.py)
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    export FLASK_DEBUG=1
    python openeogeotrellis/deploy/local.py


For production, a gunicorn server script is available:
PYTHONPATH=. python openeogeotrellis/server.py 

### Running on the Proba-V MEP
The web application can be deployed by running:
sh scripts/submit.sh
This will package the application and it's dependencies from source, and submit it on the cluster. The application will register itself with an NginX reverse proxy using Zookeeper.


### Running the unit tests

The unit tests expect that environment variable `SPARK_HOME` is set,
which can easily be done from within your development virtual environment as follows:

    export SPARK_HOME=$(find_spark_home.py)
    pytest

Run specific test or subset of test: use `-k` option, e.g. run all tests with "udp" in function name:

    pytest -k udp

To disable capturing of `print` and logging, use something like this:

    pytest --capture=no --log-cli-level=INFO
