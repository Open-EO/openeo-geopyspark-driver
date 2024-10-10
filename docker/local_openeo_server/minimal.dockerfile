# syntax=docker/dockerfile:1.2

# TODO: Add build-arg for python version
# TODO: FIX: would prefer the slim version "python:3.8-slim-bookworm" but then gdal wheels can't be built.
# At the time of writing, the latest debian version was: debian 12 bookworm
FROM python:3.8-bookworm AS base

# TODO: [1] should these env vars become an ARG so we can override it in docker build?
# ARG GDAL_VERSION=3.6.2

# TODO: Maybe this patter is better for TODO[1]: provide a build arg and put it in an ENV
ARG BUILDARG_GDAL_VERSION=3.6.2
ENV GDAL_VERSION=${BUILDARG_GDAL_VERSION}

ARG BUILDARG_JAVA_VERSION=17
ENV JAVA_VERSION=${BUILDARG_JAVA_VERSION}

ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Source directory, for source code (or app directory if you will)
ENV SRC_DIR=/src
WORKDIR $SRC_DIR
VOLUME $SRC_DIR

COPY docker/apt-install-dependencies.sh .

#
# For all RUN statements that use --mount:
#   Keep --mount options on the first line, and put all bash commands
#   on the lines below.
#   This is necessary to be able to autogenerate minimal-no-buildkit.dockerfile
#   from minimal.dockerfile with an automated find-replace of the RUN statement.
#
RUN --mount=type=cache,target=/var/cache/apt/ \
    ./apt-install-dependencies.sh

# TODO: DECIDE: pip package versions for reproducibility or prefer to have the latest, at least for pip?
#   normally it is best to pin package versions for reproducibility but maybe not for pip.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip==23.3 wheel==0.41 pip-tools==7.3


# GDAL is always a difficult one to install properly so we do this early on.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir pygdal=="$(gdal-config --version).*"


ENV REQUIREMENTS_FILE_DOCKER=requirements-docker.txt


# ==============================================================================
# Final build stage: the application to test
# ==============================================================================

FROM base AS final

# To be added: a user for running tests, and that user should own certain directories.
#    See also: tests\pre-test.sh
# RUN adduser jenkins -uid 900 --group --system

RUN mkdir /eodata

# RUN chown jenkins /eodata


#
# Install app's Python dependencies
#

COPY docker/${REQUIREMENTS_FILE_DOCKER} ${REQUIREMENTS_FILE_DOCKER}

RUN --mount=type=cache,target="~/.cache/pip" \
    python3 -m pip --no-cache-dir install -r  "${REQUIREMENTS_FILE_DOCKER}"


#
# Dev install of the application itself
#

COPY . $SRC_DIR

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-openeo/simple


# TODO: decide: do we integrate getting jars inside the docker file or leave it up to the Makefile?


# The Flask app should allow to connect from outside the Docker container,
# otherwise the connections from the host machine won't be able to reach the app.
# Remember that "localhost" *inside* a Docker container refers to the
# container itself, and not to the host machine.
ENV OPENEO_DEV_GUNICORN_HOST="0.0.0.0"


# If you want to use a different layer catalog, override the value of OPENEO_CATALOG_FILES.
ENV OPENEO_CATALOG_FILES=${SRC_DIR}/docker/example_layercatalog.json


# The web app listens on TCP port 8080, and PySpark listens on TCP port 4040
EXPOSE 8080
EXPOSE 4040


# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "openeogeotrellis/deploy/local.py"]


# TODO: what is the best way to keep the debugpy dependency for VSCode optional?
#   Would like debugpy for debugging in vscode, but should keep it optional because PyCharm users don't need it.
# TODO: rename stage "final" to something more correct and neutral
FROM final as debugwithvscode

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir debugpy==1.8.0

EXPOSE 5678

CMD ["python", "-m", "debugpy", "--listen", "0.0.0.0:5678", "openeogeotrellis/deploy/local.py"]
