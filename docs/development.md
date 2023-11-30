# Development
## Setup virtual environment
For development, it is best to combine the virtual environment from the openeo-python-driver
and the openeo-geopyspark-driver projects. By using the -e flag, you can install the two projects in the venv
using symbolic links, so that you can edit the code and see the changes immediately. Optionally, you can also
include the python client using symbolic links. Just be sure to occasionally pull the latest changes from all three
projects if you choose to do so.
```commandline
git clone --recursive git@github.com:Open-EO/openeo-geopyspark-driver.git
git clone --recursive git@github.com:Open-EO/openeo-python-driver.git
git clone git@github.com:Open-EO/openeo-python-client.git
cd openeo-geopyspark-driver
python -m venv venv
source venv/bin/activate
cd ../openeo-python-client
pip install -e .
cd ../openeo-python-driver
pip install -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-openeo/simple
cd ../openeo-geopyspark-driver
pip install -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-openeo/simple
```
## Running locally
You can run the service with:
```commandline
export SPARK_HOME=$(find_spark_home.py)
export HADOOP_CONF_DIR=/etc/hadoop/conf
export FLASK_DEBUG=1
python openeogeotrellis/deploy/local.py
```
