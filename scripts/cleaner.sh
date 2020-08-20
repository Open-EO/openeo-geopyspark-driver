#!/usr/bin/env bash

set -eo pipefail

# kinit -V -kt /root/mep_nifi.keytab mep_nifi@VGT.VITO.BE

source venv36/bin/activate
SPARK_HOME=$(find_spark_home.py) python -m openeogeotrellis.cleaner 2>&1
