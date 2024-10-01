# The base image is buiild from vito CI/CD pipeline:
FROM vito-docker.artifactory.vgt.vito.be/openeo-geotrellis-kube:latest
# sudo docker build -t openeo_docker_local .

USER root
# TODO: fix yum installs
#RUN yum install -y sshfs

# from openeo-deploy/mep/entrypont.sh
ENV JAVA_HOME=/usr/lib/jvm/jre
ENV PYTHON_EGG_CACHE=./
ENV PYSPARK_PYTHON="/opt/venv/bin/python"
ENV WMTS_BASE_URL_PATTERN=http://openeo.vgt.vito.be/openeo/services/%s
ENV PYTHONPATH="/opt/venv/lib64/python3.8/site-packages:/opt/venv/lib/python3.8/site-packages:/opt/tensorflow/python38/2.8.0:/usr/lib/python3.8/site-packages:/usr/lib64/python3.8/site-packages"
ENV LD_LIBRARY_PATH="/opt/venv/lib64"

# from jenkinslib/resources/python/test.sh
ENV PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
ENV PYTHONPATH="$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

RUN python3 -m pip config --global unset global.extra-index-url && \
    python3 -m pip config --global unset global.index-url && \
    python3 -m pip install --root-user-action=ignore pytest-runner && \
    python3 -m pip install --root-user-action=ignore pytest-timeout && \
    python3 -m pip install --root-user-action=ignore junitparser && \
    python3 -m pip install --root-user-action=ignore rioxarray && \
    python3 -m pip install --root-user-action=ignore dirty_equals && \
    python3 -m pip install --root-user-action=ignore requests_mock && \
    python3 -m pip install --root-user-action=ignore moto

# test python packages:
RUN python3 -c "import pyspark;import openeo;import otbApplication"


# pull openeo-geopyspark-driver in the docker container, just for the test code:
RUN cd /opt && \
    curl -L -O https://github.com/Open-EO/openeo-geopyspark-driver/archive/refs/heads/master.zip && \
    unzip master.zip && \
    mv openeo-geopyspark-driver-master openeo-geopyspark-driver && \
    # chmod to avoid warnings when job is done
    chmod +777 /opt/openeo-geopyspark-driver/ && \
    # delete files to be sure they don't get used:
    rm -f /opt/openeo-geopyspark-driver/tests/test_*.py && rm -rf /opt/openeo-geopyspark-driver/openeogeotrellis && \
    mkdir /opt/openeo-geopyspark-driver/jars/ && \
    ln -s /opt/geotrellis-extensions-static.jar /opt/openeo-geopyspark-driver/jars/ && \
    ln -s /opt/openeo-logging-static.jar /opt/openeo-geopyspark-driver/jars/ && \
    echo done

COPY test_run_graph.py /opt/openeo-geopyspark-driver/tests
RUN echo "[]" > /opt/openeo-geopyspark-driver/tests/layercatalog.json

# for logging:
ENV OPENEO_BATCH_JOB_ID="unknown-job"
ENV OPENEO_USER_ID="openeo_docker_local"
ENV OPENEO_LOCAL_DEBUGGING="true"

# Write directly to the mount. If an error occurs, there is something to debug with
# openeo.log will be written in the current directory, so change current directory
RUN mkdir /opt/docker_mount
WORKDIR /opt/docker_mount
CMD ["python3", "-m", "pytest", "/opt/openeo-geopyspark-driver/tests/test_run_graph.py"]
