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
    'scipy>=1.3.0',
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
        'openeo>=0.4.8a2.*',
        'openeo_driver>=0.6.2a1.*',
        'openeo_udf>=1.0.0rc3',
        'pyspark>=2.3.1,<2.4.0',
        'geopyspark==0.4.6+openeo',
        'py4j',
        'numpy>=1.17.0',
        'pandas>=0.24.2',
        'matplotlib>=2.0.0,<3.0.0',
        'geopandas==0.7.0',
        'pyproj<2.2.0',
        'protobuf==3.6.0',
        'kazoo==2.5.0',
        'rasterio==1.1.1',
        'h5py<3.0.0',
        'h5netcdf',
        'requests==2.24',
        'python_dateutil',
        'pytz',
        'affine',
        'xarray>=0.12.3',
        'Shapely',
    ],
    extras_require={
        "dev": tests_require,
    },
)
