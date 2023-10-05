#
# This makefile is intended to work on Windows and Linux, and in bash and PowerShell.
# Please stick to portable makefile constructs as much as possible.
#
# There are a few places where I already break this rule, namely the targets
# that display help info, but I plan to eliminate or reduce that as much as possible.
# Having a help function to make this makefile is just for convenience and not
# crucial for the build to work.
#


# ==============================================================================
# Configuration variables
# ==============================================================================
# Override these by creating the file "make-env.env", in dotenv format, i.e.
# key=value
# as described in docker\README.md
# ==============================================================================

# For testing purposes, we are using different image names in the makefile versus the docker-compose file.
# TODO: When makefile and docker-compose is fully equivalent we should make the image names the same.
# TODO: Decide: only offer docker-compose? Or only Makefile? Or both? Which one is the easiest?
# TODO: One way to simplifiy things is to have the makefile use docker-compose instead of pure docker-build commands.
docker_image_basename := openeo-geopyspark-driver
docker_tag := latest


#
# buildkit_supported:
#
# This lets you choose between the version of the dockerfile that leverages
# buildkit's `RUN --mount` syntax to reduces rebuild times and the
# version that leaves out `--mount` if your docker version doesn't support that.
# But on Terrascope we have an old Docker version where this does not work.
# And we can expect some people may be stuck with locked-down old docker
# versions as well.
#
# Allowed values for buildkit_supported are : 1 and 0
#
buildkit_supported := 1

# ==============================================================================
# Load file with environment variables, to override default config
# ==============================================================================

# The Makefile itself is located in the folder `docker` at the root of the workspace.
my_docker_dir := $(dir $(realpath $(MAKEFILE_LIST)))

# Now load variables from .env file if it exists
_makenv_file := $(realpath $(my_docker_dir)/make-env.env)
ifneq (,$(_makenv_file))
	include $(_makenv_file)
	export
endif


# ==============================================================================
# Derived variables and any other setup that is not configuration
# ==============================================================================

docker_image := $(docker_image_basename):$(docker_tag)
docker_image_base_stage := $(docker_image_basename)-base:$(docker_tag)

# Find the workspace directory
my_workspace_dir := $(realpath  $(my_docker_dir)/..)

# Path to the dockerfile.
# Normally we want the version that use buildkit and RUN --mount"
# But if your Docker installation does not allow it we have a nearly identical
# dockerfile that leaves out the --mount option in the RUN commands.
docker_file_minimal_nobuildkit := $(realpath $(my_docker_dir)/minimal-no-buildkit.dockerfile)
docker_file_minimal_with_buildkit := $(realpath $(my_docker_dir)/minimal.dockerfile)
ifeq ($(buildkit_supported),1)
docker_file_minimal := $(docker_file_minimal_with_buildkit)
else
docker_file_minimal := $(docker_file_minimal_nobuildkit)
endif


# Path to the requirements file for docker container, to be exported.
docker_requirements_file := $(realpath $(my_docker_dir)/requirements-docker.txt)


## ==============================================================================
## Make targets
## ==============================================================================

# In this case we want the defautl target to be a safe target that doesn't build or overwrite anything.
.PHONY: default
default: help
## Default target is: show help

.PHONY: all
all: build test
## Build and run the unit tests


## ------------------------------------------------------------------------------
## Targets for showing help and displaying setup information
## ------------------------------------------------------------------------------

.PHONY: help
help:
## Show this help.
##  This command display any comment lines that start with a double hash: ##
##  That distiguishes them from regular comments, which we do not want to show.
##  The main goal is to show the first lines (double-hashed) after a target,
## 	before the target's commands are listed, a bit like a docstrings in Python.
##  But you can also show the other double-hashed comments which show your the
##  grouping/organisation of the make targets.


ifeq ($(OS),Windows_NT)
help: help-rg
else
help: help-grep
endif


.PHONY: help-grep
help-grep:
## Show this help (for Linux and git-bash, requires grep and sed)
# This command shows the doubles hashed lines and 1 line above it, where the target should be defined.
	@grep -B1 -h -e '^\#\#' $(MAKEFILE_LIST) | sed -E -s 's/^## ([=\-])/\1/g' | sed -s 's/^##/  /g' | sed -e 's/^\-\-$$//g'


.PHONY: help-rg
help-rg:
## Show this help, using ripgrep (Alternative if you don't have or don't want grep and sed)
# This command show the doubles hashed lines and 1 line above it, where the target should be defined.
	@rg -B1 -N -e'^\#\#' $(MAKEFILE_LIST) --context-separator="" -r '  '

#@rg -B1 -N -e'^\#\#' $(MAKEFILE_LIST) --context-separator="" | sd '## -' ''  | sd '##' '  ' | sd 'docker/Makefile[:\-]' ''

.PHONY: show-vars
show-vars:
## Show the most important variables, for troubleshooting the makefile.

	@echo ""
	@echo "=== Configuration variables ==="
	@echo docker_image_basename=$(docker_image_basename)
	@echo origin docker_image_basename: $(origin docker_image_basename)
	@echo origin docker_tag: $(origin docker_tag)
	@echo origin FOO: $(origin FOO)

	@echo origin MAKEFILE_LIST: $(origin MAKEFILE_LIST)
	@echo origin SHELL: $(origin SHELL)
	@echo origin CURDIR: $(origin CURDIR)

	@echo ""
	@echo "=== Derived variables ==="
	@echo docker_image=$(docker_image)
	@echo docker_image_base_stage=$(docker_image_base_stage)

	@echo my_docker_dir=$(my_docker_dir)
	@echo my_workspace_dir=$(my_workspace_dir)
	@echo docker_file_minimal=$(docker_file_minimal)
	@echo docker_file_minimal_nobuildkit=$(docker_file_minimal_nobuildkit)
	@echo docker_file_minimal_with_buildkit=$(docker_file_minimal_with_buildkit)
	@echo buildkit_supported=$(buildkit_supported)
	@echo docker_requirements_file=$(docker_requirements_file)
	@echo path to make environment file, make-env.env (if it exists)=$(_makenv_file)

	@echo ""
	@echo "=== Variable from make ==="
	@echo CURDIR=$(CURDIR)
	@echo MAKEFILE_LIST=$(MAKEFILE_LIST)


show-image-names:
## Show the names of the docker image for all the stages/targets in the multi-stage build, for troubleshooting the makefile.
	@echo base-image stage:
	@echo $(docker_image_base_stage)
	@echo final stage:
	@echo $(docker_image)



.PHONY: help-pip-compile
help-pip-compile:
## Show pip-compile help (is actually more of a test for the docker container)
	docker run  --rm -ti --entrypoint pip-compile $(docker_image_base_stage) --help


## ------------------------------------------------------------------------------
## Targets for generating derived files
## ------------------------------------------------------------------------------


.PHONY: no-buildkit-dockerfile
no-buildkit-dockerfile:
## Create the dockerfile for systems that don't have BuildKit support.
## TODO: This target requires sd instead of sed. Get this working with regular sed, or commit to using the replacements consitently.
## 	You can Install sd via `scoop install` or `cargo install sd`.
#
# TODO: This version of the command, using sd.exe, does not work in PowerShell. Find out why.
	sd -p '^RUN \-\-mount.*' 'RUN \\' $(docker_file_minimal_with_buildkit) > $(docker_file_minimal_nobuildkit)


.PHONY: no-buildkit-dockerfile-sed
no-buildkit-dockerfile-sed:
# version with sed
# Not sure why GNU make messes up the regex syntax here. sed doesn't receive the command I expect.
	sed -E -e 's/^RUN --mount.*/RUN \\/g' $(docker_file_minimal_with_buildkit) > $(docker_file_minimal_nobuildkit)


## ------------------------------------------------------------------------------
## Targets for building
## ------------------------------------------------------------------------------

.PHONY: build
build: build-base pip-compile build-main get-jars
## Run all the build steps in order.


.PHONY: build-main
build-main:
## Build the main docker image.
	docker buildx build -t $(docker_image) -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: build-base
build-base:
## Build the main docker image.
	docker buildx build -t $(docker_image_base_stage) --target base -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: pip-compile
pip-compile:
## Run pip-compile in its docker container.
	docker run  --rm -ti \
		-v $(my_workspace_dir):/src \
		$(docker_image_base_stage) pip-compile \
		-o ./requirements-docker.txt  \
		--verbose \
		-P rlguard-lib@git+https://github.com/sentinel-hub/rate-limiting-guard.git@master#subdirectory=lib \
		--extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple \
		setup.py


.PHONY: get-jars
get-jars:
## Build the docker image for pip-tools.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) python3 scripts/get-jars.py


## ------------------------------------------------------------------------------
## Targets for testing
## ------------------------------------------------------------------------------

# We don't want to rebuild every time we run pytest.
# That's what mapping the source directory to a docker volume serves for, to avoid that long rebuild.
.PHONY: test
test:
## Run the entire pytest test suite.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) pytest -ra -vv


.PHONY: brief-test
brief-test:
## Run the entire pytest test suite.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) pytest -ra -vv tests/test_utils.py


.PHONY: run-webapp
run-webapp:
## Run the web application (the REST API Backend).
	docker run --rm -ti -v $(my_workspace_dir):/src -p 4040:4040 -p 8080:8080 $(docker_image) python openeogeotrellis/deploy/local.py


run-webapp-override-catalog:
## Run the web application (the REST API Backend) but provide the OPENEO_CATALOG_FILES env var to override the default catalog.
## This only works if you set OPENEO_CATALOG_FILES in your make-env.env file.
	docker run -e OPENEO_CATALOG_FILES=$(OPENEO_CATALOG_FILES) --rm -ti -v $(my_workspace_dir):/src -p 4040:4040 -p 8080:8080 $(docker_image) python openeogeotrellis/deploy/local.py


## ------------------------------------------------------------------------------
## Helpful but less important targets:
## to make it easy to run bash in containers for troubleshooting and debugging.
## ------------------------------------------------------------------------------

.PHONY: bash
bash:
## Open a bash shell in a fresh docker container, for debugging.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) bash


## ------------------------------------------------------------------------------
## Targets for cleaning up
## ------------------------------------------------------------------------------

.PHONY: clean
clean:
## Remove the docker images that were built via this makefile
	docker image ls -q $(docker_image) && docker rmi $(docker_image)
	docker image ls -q $(docker_image_base_stage) && docker rmi $(docker_image_base_stage)
