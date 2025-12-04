# Simple Dockerfile to package this example STAC collection as a Docker image.
# For example to be used in a dummy CWL workflow that produces a STAC collection.
# docker build -t openeo-geopyspark-driver-example-stac-catalog:1.8 .
# docker login vito-docker.artifactory.vgt.vito.be
# docker tag openeo-geopyspark-driver-example-stac-catalog:1.8 vito-docker.artifactory.vgt.vito.be/openeo-geopyspark-driver-example-stac-catalog:1.8
# docker push vito-docker.artifactory.vgt.vito.be/openeo-geopyspark-driver-example-stac-catalog:1.8

FROM python:3.11

RUN pip install pystac jsonschema
RUN mkdir /data
COPY collection.json openEO*.tif openEO*.json *.py /data/
