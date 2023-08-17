# At the time of writing, the latest debian version was: debian 12 bookworm
FROM python:3.8-bookworm AS base

# TODO: should these env vars become an ARG so we can override it in docker build?
ARG GDAL_VERSION=3.6.2
ARG JAVA_VERSION=17

ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1


RUN apt-get update \
    && apt-get install -y openjdk-${JAVA_VERSION}-jdk \  
    && apt-get install -y proj-bin proj-data \
    && apt-get install -y gdal-bin gdal-data libgdal-dev \
    && apt-get clean

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel pip-tools

# GDAL is always a difficult one to install properly so we do this early on.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir pygdal=="`gdal-config --version`.*"


# Source directory, for source code (or app directory if you will)
ENV SRC_DIR=/src
WORKDIR $SRC_DIR
VOLUME $SRC_DIR

ENV REQUIREMENTS_FILE_DOCKER=./requirements-docker.txt


# ==============================================================================
# Build stage for pip-tools / pip-compile
# ==============================================================================
#
# This stage is for running pip-compile to generate a requirements.txt
#
# Using a requirements.txt lets us have an image layer for the applications
# Python dependencies, so we don't have to do a long rebuild that installs
# all python packages each time you change one letter to your code.
#
# At present, if you only look at this dockerfile it may look like this stage
# does not do anything useful, but that is not true. 
# It has to be defined, but the way does its work is through the Makefile
# which runs the pip-compile command and runs that in the correct order before
# building the main docker image.
#
# This is not ideal but it is better than waiting for a build that take minutes
# each time you have changed one line of code.
#
# Ideally we would switch to something like pip-tools with a requirements.in file
# and generating requirements files specific to the platform
# (Docker container, Linux, Windows)
# That would make this step less cumbersome. (Or maybe TOML files can do the same)

FROM base AS piptools


#
# TODO: the approach below does not work, not unless you copy the entire source
# into this layer, and that defeats the purpose, because again each change to 
# the code triggers a rebuild that is not necessary.
#
## This does not help
## COPY setup.py setup.py
## It would needs a full copy of the source this to work and we don't want that:
# COPY . $SRC_DIR


#
# Create a requirements file from the setup.py file.
# For rlguard-lib we override it because there isn't a public package/wheel for 
# it, but you can get it directly from GitHub.
#
# Requirements files are often specific for the OS and for the Python version
# because resolving depencies gives different results per OS and Python version.
# Therefore we put it in the docker directory and name it accordingly.
#

# RUN pip-compile -o ./requirements-docker.txt  \
#     --verbose \
#     --pip-args use-pep517 \
#     -P rlguard-lib@git+https://github.com/sentinel-hub/rate-limiting-guard.git@master#subdirectory=lib \
#     --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple \
#     setup.py


CMD [ "pip-compile", "--help"]


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

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip --no-cache-dir install -r  ${REQUIREMENTS_FILE_DOCKER}


#
# Dev install of the application itself
#

COPY . $SRC_DIR

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir -e .[dev] --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple


# TODO: decide: do we integrate getting jars inside the docker file or leave it up to the Makefile?


# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "openeogeotrellis/deploy/local.py"]