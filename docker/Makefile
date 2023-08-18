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
# Override these by creating the file "makenv.env", in dotenv format, i.e.
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
buildkit_supported := 0

# ==============================================================================
# Load file with environment variables, to override default config
# ==============================================================================

# The Makefile itself is located in the folder `docker` at the root of the workspace.
my_docker_dir := $(dir $(realpath $(MAKEFILE_LIST)))

# Now load variables from .env file if it exists
_makenv_file := $(realpath $(my_docker_dir)/makenv.env)
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
ifeq ($(buildkit_supported),1)
docker_file_minimal := $(realpath $(my_docker_dir)/minimal.dockerfile)
else
docker_file_minimal := $(realpath $(my_docker_dir)/minimal-no-buildkit.dockerfile)
endif

# Path to the requirements file for docker container, to be exported.
docker_requirements_file := $(realpath $(my_docker_dir)/requirements-docker.txt)


# ==============================================================================
# Make targets
# ==============================================================================

# In this case we want the defautl target to be a safe target that doesn't build or overwrite anything.
.PHONY: default
default: help
## Default target is: show help

.PHONY: all
all: build test
## Build and run the unit tests


# ------------------------------------------------------------------------------
# Targets for showing help and displaying setup information
# ------------------------------------------------------------------------------

.PHONY: help
help:
## Show this help. This command looks for comments on the first line after a target
## and these comments should start with a double hash ##  to distinguish them
## from regular comments. Much like "docstrings" as we call it in Python.

ifeq ($(OS),Windows_NT)
help: help-rg
else
help: help-grep
endif


.PHONY: help-grep
help-grep:
## Show this help (for Linux and git-bash, requires grep and sed)
	@grep -A1 -e '^[a-z][a-zA-Z0-9\\\-]*:' $(MAKEFILE_LIST) | sed -s 's/^##/  /g' | sed -e 's/\-\-//g'


.PHONY: help-rg
help-rg:
## Show this help, using ripgrep (Alternative if you don't have or don't want grep and sed)
	@rg -A1 -N -e'^[a-z][a-zA-Z0-9\\\-]*:' $(MAKEFILE_LIST)


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
	@echo path to makenv.env (if it exists)=$(_makenv_file)

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


# ------------------------------------------------------------------------------
# Targets for building
# ------------------------------------------------------------------------------

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


# ------------------------------------------------------------------------------
# Targets for testing
# ------------------------------------------------------------------------------

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


# ------------------------------------------------------------------------------
# Helpful but less important targets:
# to make it easy to run bash in containers for troubleshooting and debugging.
# ------------------------------------------------------------------------------

.PHONY: bash
bash:
## Open a bash shell in a fresh docker container, for debugging.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) bash


# ------------------------------------------------------------------------------
# Targets for cleaning up
# ------------------------------------------------------------------------------

.PHONY: clean
clean:
## Remove the docker images that were built via this makefile
	docker image ls -q $(docker_image) && docker rmi $(docker_image)
	docker image ls -q $(docker_image_base_stage) && docker rmi $(docker_image_base_stage)
