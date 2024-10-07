# The base image is buiild from vito CI/CD pipeline:
FROM vito-docker.artifactory.vgt.vito.be/openeo-base:latest
# docker build -t openeo_docker_local . -f docker/local_batch_job/Dockerfile

USER root
# TODO: fix yum installs
#RUN yum install -y sshfs

# from openeo-deploy/mep/entrypont.sh
ENV PYTHON_EGG_CACHE=./
ENV PYSPARK_PYTHON="/opt/venv/bin/python"
ENV PATH="/opt/venv/bin:$PATH"
ENV WMTS_BASE_URL_PATTERN=http://openeo.vgt.vito.be/openeo/services/%s
ENV PYTHONPATH="/opt/venv/lib64/python3.8/site-packages:/opt/venv/lib/python3.8/site-packages:/opt/tensorflow/python38/2.8.0:/usr/lib/python3.8/site-packages:/usr/lib64/python3.8/site-packages"
ENV LD_LIBRARY_PATH="/opt/venv/lib64"

# from jenkinslib/resources/python/test.sh
ENV PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
ENV PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

# test python packages:
RUN python3 -c "import pyspark;import openeo;import otbApplication"

# COPY files to allow building the image with the latest version stored locally:
# TODO: Disable this by default when a new openeo-base is available.
COPY ./openeogeotrellis /opt/venv/lib64/python3.8/site-packages/openeogeotrellis
COPY ./openeogeotrellis/deploy/run_graph_locally.py /opt/venv/lib64/python3.8/site-packages/openeogeotrellis/deploy/run_graph_locally.py
COPY ./openeogeotrellis/deploy/local.py /opt/venv/lib64/python3.8/site-packages/openeogeotrellis/deploy/local.py
COPY ./openeogeotrellis/deploy/local_backend_config.py /opt/venv/lib64/python3.8/site-packages/openeogeotrellis/deploy/local_backend_config.py

# to find jars:
ENV GEOPYSPARK_JARS_PATH=/opt/

# for logging:
ENV OPENEO_BATCH_JOB_ID="unknown-job"
ENV OPENEO_USER_ID="openeo_docker_local"
# When debugging is enabled, port 4040 and 5009 are exposed:
ENV OPENEO_LOCAL_DEBUGGING="false"

# Write directly to the mount. If an error occurs, there is something to debug with
# openeo.log will be written in the current directory, so change current directory
RUN mkdir /opt/docker_mount
WORKDIR /opt/docker_mount
# TODO: Use /opt/venv/bin/run_graph_locally.py when a new openeo-base is available:
ENTRYPOINT ["python3", "-u", "/opt/venv/lib64/python3.8/site-packages/openeogeotrellis/deploy/run_graph_locally.py"]
