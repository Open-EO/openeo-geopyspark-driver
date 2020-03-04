from setuptools import setup,find_packages
import os
import datetime

# Load the openeo version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version

__version__ = None
date = datetime.datetime.today().strftime('%Y%m%d')

with open('openeogeotrellis/_version.py') as fp:
    exec(fp.read())

version = __version__

setup(
    name='openeo-geopyspark',
    version=version,
    packages=find_packages(exclude=('tests', 'scripts')),
    include_package_data = True,
    data_files=['layercatalog.json','scripts/log4j.properties'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest','mock'],
    install_requires=[
        'flask',
        'openeo>=0.0.8',
        'openeo_udf>=0.0.9.post0',
        'matplotlib>=2.0.0,<3.0.0',
        'colortools>=0.1.2',
        'geopandas==0.3.0',
        'geopyspark==0.4.5+openeo',
        'protobuf==3.6.0',
        'cChardet',
        'gunicorn==19.9.0',
        'kazoo==2.4.0',
        'flask-cors',
        'rasterio==1.1.1'
    ],
)
