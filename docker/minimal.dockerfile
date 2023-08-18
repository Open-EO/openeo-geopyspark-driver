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

RUN --mount=type=cache,target=/var/cache/apt/ ./apt-install-dependencies.sh

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel pip-tools

# GDAL is always a difficult one to install properly so we do this early on.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir pygdal=="$(gdal-config --version).*"


ENV REQUIREMENTS_FILE_DOCKER=./requirements-docker.txt


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

COPY ${REQUIREMENTS_FILE_DOCKER} ${REQUIREMENTS_FILE_DOCKER}

RUN --mount=type=cache,target="~/.cache/pip" \
    python3 -m pip --no-cache-dir install -r  "${REQUIREMENTS_FILE_DOCKER}"


#
# Dev install of the application itself
#

COPY . $SRC_DIR

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple


# TODO: decide: do we integrate getting jars inside the docker file or leave it up to the Makefile?


# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "openeogeotrellis/deploy/local.py"]
