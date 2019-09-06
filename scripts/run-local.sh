#!/usr/bin/env bash

# run it from the root of the openeo-gepyspark-driver
echo $(which python3)

export SPARK_HOME=$(find_spark_home.py)
#export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLASK_DEBUG=1
export DRIVER_IMPLEMENTATION_PACKAGE=openeogeotrellis
export ZOOKEEPERNODES=localhost:2181

#export PYTHONPATH=../openeo-python-driver/:../openeo-client-api:./:../geopyspark SPARK_HOME=$(find_spark_home.py)

#export PYTHONPATH=../openeo-udf/src/:../openeo-python-client/:../openeo-python-driver/:../openeo-client-api:./

export PYTHONPATH=../install/

python3 openeogeotrellis/deploy/local.py
