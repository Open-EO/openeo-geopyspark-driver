#!/bin/sh
# pre-test procedure specific for VITO Jenkins CI toolchain
# executed just before running the tests

set -eux
pwd

# TODO: is this `/eodata` still necessary or can this be eliminated?
mkdir /eodata
chown jenkins /eodata

runuser jenkins -c '
set -eux
source ./env.sh
python3 scripts/get-jars.py --force-download --python-version ${PYTHON_VERSION:-3.8} jars
'

# Root folder for temp folders during tests (e.g. through `tmp_path` fixture).
# Assigned to `PYTEST_DEBUG_TEMPROOT` from Jenkinsfile.
mkdir -p pytest-tmp
chown jenkins pytest-tmp
