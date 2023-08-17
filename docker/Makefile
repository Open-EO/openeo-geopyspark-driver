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

# For testing purposes, we are using different image names in the makefile versus the docker-compose file.
# TODO: When makefile and docker-compose is fully equivalent we should make the image names the same.
# TODO: Decide: only offer docker-compose? Or only Makefile? Or both? Which one is the easiest?
# TODO: One way to simplifiy things is to have the makefile use docker-compose instead of pure docker-build commands.
docker_image_basename := openeo-geopyspark-driver
docker_tag := latest

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
docker_image_piptools_stage := $(docker_image_basename)-piptools:$(docker_tag)
docker_image_base_stage := $(docker_image_basename)-base:$(docker_tag) 

# Find the workspace directory
my_workspace_dir := $(realpath  $(my_docker_dir)/..)

# Path to the dockerfile.
docker_file_minimal := $(realpath $(my_docker_dir)/minimal.dockerfile)

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


# ------------------------------------------------------------------------------
# Targets for showing help and displaying setup information
# ------------------------------------------------------------------------------


.PHONY: help
## Show this help (for Linux and git-bash, requires grep and sed)
# ifeq ($(has_grep), 1)
ifeq ($(OS),Windows_NT)
help: help-rg
else
help: help-grep
	@grep -A1 -e '^[a-z][a-zA-Z0-9\\\-]*:' $(MAKEFILE_LIST) | sed -s 's/^##/  /g' | sed -e 's/\-\-//g'
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
	@echo docker_image_piptools_stage=$(docker_image_piptools_stage)
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
	@echo ""
	@echo piptools-image stage: 
	@echo $(docker_image_piptools_stage)
	@echo ""
	@echo final stage: 
	@echo $(docker_image)



.PHONY: help-pip-compile
help-pip-compile:
## Show pip-compile help (is actually more of a test for the docker container)
	docker run  --rm -ti --entrypoint pip-compile $(docker_image_piptools_stage) --help


# ------------------------------------------------------------------------------
# Targets for building
# ------------------------------------------------------------------------------


.PHONY: build
# build: build-main
build: build-piptools pip-compile build-main get-jars
## Run all the build steps in order.


.PHONY: build-main
build-main:
## Build the main docker image.
	docker build -t $(docker_image) -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: build-piptools
build-piptools:
## Build the docker image for pip-tools (target: piptools)
	docker build -t $(docker_image_piptools_stage) --target piptools -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: pip-compile
pip-compile:
## Run pip-compile in its docker container.
	docker run  --rm -ti -v $(my_workspace_dir):/src $(docker_image_piptools_stage) pip-compile \
		-o ./requirements-docker.txt  \
		--verbose \
		--pip-args use-pep517 \
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


# TODO: bash-piptools is temporary, to troubleshoot the pip-tools container. 
.PHONY: bash-piptools
bash-piptools:
## Open a bash shell in the pip-tools docker container
	docker run  -ti  --entrypoint bash  --rm  -v $(my_workspace_dir):/src  $(docker_image_piptools_stage)


# ------------------------------------------------------------------------------
# Targets for cleaning up
# ------------------------------------------------------------------------------

.PHONY: clean
clean:
## Remove the docker images that were built via this makefile
	@echo "docker image ls -q $(docker_image) && docker rmi $(docker_image)"
	@echo "docker image ls -q $(docker_image_piptools_stage) && docker rmi $(docker_image_piptools_stage)"
	@echo "docker image ls -q $(docker_image_base_stage) && docker rmi $(docker_image_base_stage)"
	
