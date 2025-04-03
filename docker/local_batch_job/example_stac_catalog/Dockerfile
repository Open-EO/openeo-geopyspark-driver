# Simple Dockerfile to package this example STAC collection as a Docker image.
# For example to be used in a dummy CWL workflow that produces a STAC collection.

FROM alpine:latest

RUN mkdir /data
COPY collection.json openEO*.tif openEO*.json /data
