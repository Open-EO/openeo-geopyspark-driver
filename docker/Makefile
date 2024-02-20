# ##############################################################################
#
# This Makefile makes some repetitive docker build/run/... commands easier.
#
# Further, it automates generating a few files:
#   - docker/requirements-docker.txt, built by command: `make pip-compile`
#	- docker/minimal-no-buildkit.dockerfile, built by command: `no-buildkit-dockerfile`
#
# ##############################################################################
#
# Please stick to portable makefile constructs as much as possible.
# This makefile is intended to work on Windows and Linux, and in bash as well as
# in PowerShell.
#
# There are a few places where I already break this rule, namely the targets
# that display help info, but I plan to eliminate or reduce that as much as possible.
# Having a help function to make this makefile is just for convenience and not
# crucial for the build to work.
#
# Note that any comments that start with a double hash, like so '##', will
# be displayed by the `make help` command.
#
# ##############################################################################



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


# ==============================================================================
# Load file with environment variables, to override default config
# ==============================================================================

# The Makefile itself is located in the folder `docker` at the root of the workspace.
my_docker_dir := $(dir $(realpath $(MAKEFILE_LIST)))

# Now load variables from make-env.env file if it exists
# TODO: consider eliminating make-env.env because a lot of this is moving to .env
# 	which is a file that docker-compose needs anyway. Perhaps .env can contain everything.
_rawpath_makenv_file = $(join $(my_docker_dir), make-env.env)
_makenv_file := $(realpath $(_rawpath_makenv_file))
ifneq (,$(_makenv_file))
	include $(_makenv_file)
	export
endif

# Now load variables from .env file meant for docker-compose, if it exists
_rawpath_docker_env_file := $(join $(my_docker_dir), .env)
_docker_env_file := $(realpath $(_rawpath_docker_env_file))
ifneq (,$(_docker_env_file))
	include $(_docker_env_file)
	export
endif


# ==============================================================================
# defaults, when variable is not defined
# ==============================================================================

OPENEO_CATALOG_FILES ?= /src/docker/example_layercatalog.json
DATA_PATH_ON_HOST ?= $(join $(my_docker_dir), container-data)

# ------------------------------------------------------------------------------
# buildkit_supported:
# ------------------------------------------------------------------------------
# This lets you choose between the version of the dockerfile that leverages
# buildkit's `RUN --mount` syntax to reduces rebuild times and the
# version that leaves out `--mount` if your docker version doesn't support that.
# But on Terrascope we have an old Docker version where this does not work.
# And we can expect some people may be stuck with locked-down old docker
# versions as well.
#
# Allowed values for buildkit_supported are : 1 and 0
#
buildkit_supported ?= 1


# ==============================================================================
# Derived variables and any other setup that is not configuration
# ==============================================================================

docker_image := $(docker_image_basename):$(docker_tag)
docker_image_base_stage := $(docker_image_basename)-base:$(docker_tag)
docker_image_debug_stage := $(docker_image_basename)-debug:$(docker_tag)

# Find the workspace directory
my_workspace_dir := $(realpath  $(my_docker_dir)/..)

# Path to the dockerfile.
# Normally we want the version that use buildkit and RUN --mount"
# But if your Docker installation does not allow it we have a nearly identical
# dockerfile that leaves out the --mount option in the RUN commands.

raw_docker_file_minimal_nobuildkit := $(my_docker_dir)/minimal-no-buildkit.dockerfile
raw_docker_file_minimal_with_buildkit := $(my_docker_dir)/minimal.dockerfile)

docker_file_minimal_nobuildkit := $(realpath $(my_docker_dir)/minimal-no-buildkit.dockerfile)
docker_file_minimal_with_buildkit := $(realpath $(my_docker_dir)/minimal.dockerfile)
ifeq ($(buildkit_supported),1)
docker_file_minimal := $(docker_file_minimal_with_buildkit)
else
docker_file_minimal := $(docker_file_minimal_nobuildkit)
endif


# Path to the requirements file for docker container, to be exported.
# docker_requirements_file := $(realpath $(my_docker_dir)/requirements-docker.txt)
docker_requirements_file := docker/requirements-docker.txt


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
	@echo docker_file_minimal_with_buildkit=$(docker_file_minimal_with_buildkit)
	@echo docker_file_minimal_nobuildkit=$(docker_file_minimal_nobuildkit)
	@echo docker_requirements_file=$(docker_requirements_file)

	@echo ""
	@echo "=== Variables that make sets automatically ==="
	@echo CURDIR=$(CURDIR)
	@echo MAKEFILE_LIST=$(MAKEFILE_LIST)

	@echo ""
	@echo "=== Variables from make_env.env ==="
	@echo "path to make environment file, make-env.env (empty if it does not exists)=$(_makenv_file)"
	@echo "expected path to make-env.env: $(_rawpath_makenv_file)
	@echo buildkit_supported=$(buildkit_supported)

	@echo ""
	@echo "=== Variables from make_env.env ==="
	@echo "path to .env docker-compose environment file (empty if it does not exists)=$(_docker_env_file)"
	@echo "expected path to .env: $(_rawpath_docker_env_file)"
	@echo DATA_PATH_ON_HOST=$(DATA_PATH_ON_HOST)
	@echo OPENEO_CATALOG_FILES=$(OPENEO_CATALOG_FILES)


show-image-names:
## Show the names of the docker image for all the stages/targets in the multi-stage build, for troubleshooting the makefile.
	@echo base-image stage:
	@echo $(docker_image_base_stage)
	@echo final stage:
	@echo $(docker_image)
	@echo debug stage:
	@echo $(docker_image_debug_stage)


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
## This target does not run in Powershell. You need to run it in bash or git-bash.
## TODO: This target requires sd instead of sed. Get this working with regular sed, or commit to using the replacements consitently.
## 	You can Install sd via `scoop install` or `cargo install sd`.
# TODO: This version of the command, using sd.exe, does not work in PowerShell. Find out why.
	sd -p '^RUN \-\-mount.*' 'RUN \\' $(docker_file_minimal_with_buildkit) > $(docker_file_minimal_nobuildkit)


.PHONY: no-buildkit-dockerfile-sed
no-buildkit-dockerfile-sed:
## Similar to no-buildkit-dockerfile but this command uses sed instead of sd.
## TODO: no-buildkit-dockerfile-sed is not working yet.
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
## Build the base docker image.
	docker buildx build -t $(docker_image_base_stage) --target base -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: build-debug
build-debug:
## Build the docker image that support debugging in VS Code, or anything else that supports debugpy.
	docker buildx build -t $(docker_image_debug_stage) --target debugwithvscode -f $(docker_file_minimal) $(my_workspace_dir)


.PHONY: pip-compile
pip-compile: $(docker_requirements_file)
## Run pip-compile in its docker container.
## This target is an alias/shorthand for $(docker_requirements_file)


# More complex expression for setup.py: so you can run make from either the root of the repo or from ./docker/
$(docker_requirements_file): $(realpath $(my_docker_dir)/../setup.py)
## Run pip-compile in its docker container.
# The --strip-extras option is going to be the default in pip-tools v8.0.0.
# Adding this now makes it behave the same way it will behave in near future.
	docker run  --rm -ti \
		-v $(my_workspace_dir):/src \
		$(docker_image_base_stage) \
		pip-compile \
		-o $(docker_requirements_file) \
		--verbose \
		--extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-openeo/simple \
		--strip-extras \
		setup.py


.PHONY: get-jars
get-jars:
## Download the latest jar files.
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
## Run just a small part of the test suite, so you can check if the image build is working.
	docker run --rm -ti -v $(my_workspace_dir):/src  $(docker_image) pytest -ra -vv tests/test_utils.py


.PHONY: run-webapp
run-webapp:
## Run the web application (the REST API Backend).
	docker run --rm -ti \
	-v $(my_workspace_dir):/src \
	-v $(DATA_PATH_ON_HOST):/data \
	-p 4040:4040 -p 8080:8080 \
	-e OPENEO_CATALOG_FILES=$(OPENEO_CATALOG_FILES) \
	$(docker_image) python openeogeotrellis/deploy/local.py


debug-webapp:
## Run the web application (the REST API Backend) and make it listen for a debugger
## that wants to connect.
## TODO: Add support for PyCharm, or other debuggers. For now this only supports debugpy, used by VSCode.
	docker run --rm -ti \
	-v $(my_workspace_dir):/src \
	-v $(DATA_PATH_ON_HOST):/data \
	-p 4040:4040 -p 8080:8080 -p 5678:5678 \
	-e OPENEO_CATALOG_FILES=$(OPENEO_CATALOG_FILES) \
	$(docker_image_debug_stage) python -m debugpy --listen 0.0.0.0:5678 openeogeotrellis/deploy/local.py


## ------------------------------------------------------------------------------
## Helpful but less important targets:
## to make it easy to run bash in containers for troubleshooting and debugging.
## ------------------------------------------------------------------------------

.PHONY: bash
bash:
## Open a bash shell in a fresh docker container, for debugging.
	docker run --rm -ti \
	-v $(my_workspace_dir):/src \
	-v $(DATA_PATH_ON_HOST):/data \
	-e OPENEO_CATALOG_FILES=$(OPENEO_CATALOG_FILES) \
	-e DATA_PATH_ON_HOST=${DATA_PATH_ON_HOST} \
	$(docker_image) bash



## ------------------------------------------------------------------------------
## Targets for cleaning up
## ------------------------------------------------------------------------------

.PHONY: clean
clean:
## Remove the docker images that were built via this makefile
	docker image ls -q $(docker_image) && docker rmi $(docker_image)
	docker image ls -q $(docker_image_base_stage) && docker rmi $(docker_image_base_stage)
	docker image ls -q $(docker_image_debug_stage) && docker rmi $(docker_image_debug_stage)
