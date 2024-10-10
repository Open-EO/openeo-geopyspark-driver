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
    'moto[s3]>=5.0.0',
    'schema',
    'requests-mock>=1.8.0',
    'openeo_udf>=1.0.0rc3',
    "time_machine>=2.8.0",
    "kubernetes",
    "re-assert",
    "dirty-equals>=0.6",
]

setup(
    name='openeo-geopyspark',
    version=version,
    python_requires=">=3.8",
    packages=find_packages(exclude=('tests', 'scripts')),
    include_package_data = True,
    data_files=[
        ("openeo-geopyspark-driver", [
            "CHANGELOG.md",
            # TODO: make these config files real "package_data" so that they can be managed/found more easily in different contexts
            "scripts/submit_batch_job_log4j.properties",
            "scripts/submit_batch_job_log4j2.xml",
            "scripts/batch_job_log4j2.xml",
            "scripts/cleaner-entrypoint.sh",
            "scripts/job_tracker-entrypoint.sh",
            "scripts/async_task-entrypoint.sh",
            "scripts/async_task_log4j2.xml",
            "scripts/kleaner-entrypoint.sh",
            "scripts/zookeeper_set.py",
        ]),
    ],
    setup_requires=['pytest-runner'],
    tests_require=tests_require,
    install_requires=[
        "openeo>=0.32.0.a2.dev",
        "openeo_driver>=0.110.0.dev",
        'pyspark==3.4.2; python_version>="3.8"',
        'pyspark>=2.3.1,<2.4.0; python_version<"3.8"',
        'geopyspark==0.4.7+openeo',
        # rasterio is an undeclared but required dependency for geopyspark
        # (see https://github.com/locationtech-labs/geopyspark/issues/683 https://github.com/locationtech-labs/geopyspark/pull/706)
        'rasterio~=1.2.0; python_version<"3.9"',
        'rasterio~=1.3.10; python_version>="3.9"',
        'py4j',
        'numpy==1.22.4; python_version<"3.9"',
        'numpy; python_version>="3.9"',
        'pandas>=1.4.0,<2.0.0; python_version<"3.9"',
        'pandas; python_version>="3.9"',
        'pyproj==3.4.1',
        'protobuf~=3.9.2',
        'kazoo~=2.8.0',
        'h5py==2.10.0; python_version<"3.9"',
        'h5py~=3.11.0; python_version>="3.9"',
        'h5netcdf',
        'requests>=2.26.0,<3.0',
        'python_dateutil',
        'pytz',
        'affine',
        'xarray~=0.16.2; python_version<"3.9"',
        'xarray~=2024.7.0; python_version>="3.9"',
        "netcdf4",
        'Shapely<2.0',
        'epsel~=1.0.0',
        'numbagg==0.1',
        'Bottleneck~=1.3.2; python_version<"3.9"',
        'Bottleneck~=1.4.0; python_version>="3.9"',
        'python-json-logger',
        'jep==4.1.1',
        'kafka-python==1.4.6',
        'deprecated>=1.2.12',
        'elasticsearch==7.16.3',
        'pystac_client~=0.7.2',
        'boto3>=1.16.25,<2.0',
        "hvac>=1.0.2",
        "pyarrow>=1.0.0",  # For pyspark.pandas
        "attrs>=22.1.0",
        "planetary-computer~=1.0.0",
        "reretry~=0.11.8",
        "traceback-with-variables==2.0.4",
        'scipy>=1.8' # used by sentinel-3 reader
    ],
    extras_require={
        "dev": tests_require,
        "k8s": [
            "kubernetes",
            "PyYAML",
        ],
        "yarn": [
            "gssapi>=1.8.0",
            "requests-gssapi>=1.2.3",  # For Kerberos authentication
        ],
    },
    entry_points={
        "console_scripts": [
            "openeo_kube.py = openeogeotrellis.deploy.kube:main",
            "openeo_batch.py = openeogeotrellis.deploy.batch_job:start_main",
            "run_graph_locally.py = openeogeotrellis.deploy.run_graph_locally:main",
        ]
    }
)
