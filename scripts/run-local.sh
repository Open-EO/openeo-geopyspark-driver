#!/usr/bin/env bash

# run it from the root of the openeo-geopyspark-driver


export SPARK_HOME=$(find_spark_home.py)
#export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLASK_DEBUG=1
export ZOOKEEPERNODES=localhost:2181

#export PYTHONPATH=../openeo-python-driver/:../openeo-client-api:./:../geopyspark SPARK_HOME=$(find_spark_home.py)

export PYTHONPATH=../openeo-udf/src/:../openeo-python-client/:../openeo-python-driver/:../openeo-client-api:./

python3 openeogeotrellis/deploy/local.py
