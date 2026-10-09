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

yarn_require = [
    "gssapi>=1.8.0",
    "requests-gssapi>=1.2.3",  # For Kerberos authentication
]

tests_require = [
    'pytest',
    'pytest-xdist',
    'pytest-timeout',
    'mock',
    'moto[s3]>=5.0.0',
    'schema',
    'requests-mock>=1.8.0',
    'openeo_udf>=1.0.0rc3',
    "time_machine>=2.8.0,<3.0.0",
    "kubernetes",
    "re-assert",
    "dirty-equals>=0.6",
    "cryptography~=46.0.0",
    "responses",
    "rio_cogeo",
    "pydantic",
    "zarr",
    "jsonschema",
    "rioxarray",
    # TODO: GDAL (aka osgeo.gdal) should be listed here. See https://github.com/Open-EO/openeo-geopyspark-driver/issues/1363
] + yarn_require

typing_require = [
    "mypy-boto3-sts",
    "mypy-boto3-s3",
    "types-shapely",
    "scipy-stubs",
    "typeguard",
]

setup(
    name='openeo-geopyspark',
    version=version,
    python_requires=">=3.11",
    packages=find_packages(exclude=('tests', 'scripts')),
    include_package_data = True,
    data_files=[
        ("openeo-geopyspark-driver", [
            "CHANGELOG.md",
            # TODO: make these config files real "package_data" so that they can be managed/found more easily in different contexts
            "scripts/submit_batch_job_log4j.properties",
            "scripts/submit_batch_job_log4j2.xml",
            "scripts/batch_job_log4j2.xml",
            "scripts/job_tracker-entrypoint.sh",
            "scripts/zookeeper_set.py",
            "scripts/job_cleaner.py",
        ]),
    ],
    tests_require=tests_require,
    install_requires=[
        "openeo>=0.48.0.a4.dev",
        "openeo_driver>=0.142.0a3.dev",
        "opentelemetry-api>=1.0.0",
        "prometheus-client>=0.20.0",
        "pyspark>=4.0.0",
        'geopyspark_openeo==0.4.3.post1',
        # rasterio is an undeclared but required dependency for geopyspark
        # (see https://github.com/locationtech-labs/geopyspark/issues/683 https://github.com/locationtech-labs/geopyspark/pull/706)
        "rasterio~=1.3.10",
        'py4j',
        "numpy>=2.3.3,<2.5",
        "pandas",
        'pyproj==3.4.1',
        'protobuf~=3.9.2',
        "kazoo~=2.11.0",
        "h5py~=3.11.0",
        'h5netcdf',
        'requests>=2.26.0,<3.0',
        'python_dateutil',
        'pytz',
        'affine',
        "xarray~=2024.7.0",
        "netcdf4",
        "shapely>=2.0.0",
        'epsel~=1.0.0',
        "Bottleneck~=1.4.0",
        "python-json-logger~=2.0",  # Avoid breaking change in 3.1.0 https://github.com/nhairs/python-json-logger/issues/29
        "jep_openeo_numpy==4.1.2; python_version == '3.11'", # Required because Jep needs to compile against numpy 2.x
        #"jep; python_version >= '3.12'", # disabled because requires java_home, TODO:build custom wheel
        'deprecated>=1.2.12',
        'elasticsearch==7.16.3',
        "pystac>=1.8.4",
        'pystac_client~=0.7.2',
        'boto3>=1.16.25,<2.0',
        "hvac>=1.0.2",
        "pyarrow>=1.0.0",  # For pyspark.pandas
        "attrs>=22.1.0",
        "planetary-computer~=1.0.0",
        "reretry~=0.11.8",
        'scipy>=1.8',  # used by sentinel-3 reader
        "PyJWT[crypto]>=2.9.0",  # For identity tokens
        "urllib3>=1.26.20",
        "geopandas>=1.0.0",
    ],
    extras_require={
        "dev": tests_require + typing_require,
        "k8s": [
            "kubernetes",
            "PyYAML",
        ],
        "yarn": yarn_require,
        "spark35": ["pyspark>=3.5.0,<4.0.0"],
        "spark4": ["pyspark>=4.0.0,<5.0.0"],
        # "extras" trick to allow pinning down on pyspark 4.0.x for particular CI contexts
        "spark40x": ["pyspark~=4.0.0"],
    },
    entry_points={
        "console_scripts": [
            "openeo_kube.py = openeogeotrellis.deploy.kube:main",
            "openeo_kube_lite.py = openeogeotrellis.deploy.kube_webapp_lite:main",
            "openeo_batch.py = openeogeotrellis.deploy.batch_job:start_main",
            "openeo_local.py = openeogeotrellis.deploy.local:main",
            "run_graph_locally.py = openeogeotrellis.deploy.run_graph_locally:main",
        ]
    }
)
