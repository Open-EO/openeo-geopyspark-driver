from setuptools import setup,find_packages

# Load the openeo version info.
#
# Note that we cannot simply import the module, since dependencies listed
# in setup() will very likely not be installed yet when setup.py run.
#
# See:
#   https://packaging.python.org/guides/single-sourcing-package-version

__version__ = None

with open('openeogeotrellis/_version.py') as fp:
    exec(fp.read())

version = __version__

tests_require = [
    'pytest',
    'mock',
    'schema',
]

setup(
    name='openeo-geopyspark',
    version=version,
    packages=find_packages(exclude=('tests', 'scripts')),
    include_package_data = True,
    data_files=[
        'layercatalog.json',
        'log4j.properties',
        'scripts/submit_batch_job_log4j.properties',
        'scripts/batch_job_log4j.properties'
    ],
    setup_requires=['pytest-runner'],
    tests_require=tests_require,
    install_requires=[
        'flask',
        'pyspark>=2.3.1,<2.4.0',
        'openeo>=0.4.3a1.*',
        'openeo_driver>=0.5.3a1.*',
        'openeo_udf>=1.0.0rc2',
        'matplotlib>=2.0.0,<3.0.0',
        'colortools>=0.1.2',
        'geopandas==0.7.0',
        'geopyspark==0.4.6+openeo',
        'protobuf==3.6.0',
        'cChardet',
        'gunicorn',
        'kazoo==2.4.0',
        'flask-cors',
        'rasterio==1.1.1',
        'pydantic',
        'h5netcdf',
        'requests==2.24'
    ],
    extras_require={
        "dev": tests_require,
    },
)
