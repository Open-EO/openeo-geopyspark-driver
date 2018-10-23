#!/usr/bin/env bash

export PYTHONPATH=../openeo-python-driver/:../openeo-client-api:./:../geopyspark SPARK_HOME=$(find_spark_home.py)
python openeogeotrellis/deploy/local.py
