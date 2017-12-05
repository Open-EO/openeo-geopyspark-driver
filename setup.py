from setuptools import setup

setup(
    name='openeo-geopyspark',
    packages=['openeogeotrellis'],
    include_package_data=True,
    install_requires=[
        'flask',
    ],
)