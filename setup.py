from setuptools import setup

setup(
    name='openeo-geopyspark',
    packages=['openeo-geopyspark'],
    include_package_data=True,
    install_requires=[
        'flask',
    ],
)