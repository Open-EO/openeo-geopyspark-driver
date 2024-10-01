#!/bin/bash

# ==============================================================================
# REQUIRED ENV VAR!
# ------------------------------------------------------------------------------
# This script expects the environment variable JAVA_VERSION to be set
# ==============================================================================


# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for
# details.
set -euo pipefail

# Tell apt-get we're never going to be able to give manual
# feedback:
export DEBIAN_FRONTEND=noninteractive

if [ -v JAVA_VERSION ];
then 
    export JAVA_VERSION=17
fi

# Update the package listing, so we know what package exist:
apt-get update

# Install security updates:
apt-get -y upgrade

# TODO: Get it to work with --no-install-recommends, need to troubleshoot what is missing.
# Install a new package, without unnecessary recommended packages:
# apt-get -y install --no-install-recommends \
apt-get -y install \
    openjdk-${JAVA_VERSION}-jdk \
    proj-bin proj-data \
    gdal-bin gdal-data libgdal-dev \

# Delete cached files we don't need anymore (note that if you're
# using official Docker images for Debian or Ubuntu, this happens
# automatically, you don't need to do it yourself):
apt-get clean

# Delete index files we don't need anymore:
rm -rf /var/lib/apt/lists/*
