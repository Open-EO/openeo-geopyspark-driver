## OpenEO Geopyspark Driver

[![Status](https://img.shields.io/badge/Status-proof--of--concept-yellow.svg)]()

Python version: 3.5

This driver implements the GeoPySpark/Geotrellis specific backend for OpenEO.

It does this by implementing a direct (non-REST) version of the OpenEO client API on top 
of [GeoPySpark](https://github.com/locationtech-labs/geopyspark/). 

A REST service based on Flask translates incoming calls to this local API.

### Running locally
For development, you can run the service using Flask:
export FLASK_APP=openeogeotrellis/__init__.py
export SPARK_HOME=/usr/lib64/python3.6/site-packages/pyspark
flask run

For production, a gunicorn server script is available:
PYTHONPATH=. python openeogeotrellis/server.py 
