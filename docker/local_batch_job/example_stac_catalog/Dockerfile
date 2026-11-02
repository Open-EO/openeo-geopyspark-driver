# Simple Dockerfile to package this example STAC collection as a Docker image.
# For example to be used in a dummy CWL workflow that produces a STAC collection.
# docker build -t openeo-geopyspark-driver-example-stac-catalog:1.9 .
# docker login <registry_url>
# docker tag openeo-geopyspark-driver-example-stac-catalog:1.9 <registry_url>/openeo-geopyspark-driver-example-stac-catalog:1.9
# docker push <registry_url>/openeo-geopyspark-driver-example-stac-catalog:1.9

FROM python:3.11

RUN pip install pystac jsonschema
RUN mkdir /data
COPY collection.json openEO*.tif openEO*.json *.py /data/
