# How to use the docker setup for development

## Goal: for development use only, or as an example for your own setup

The dockerfile and build scripts that you find in here aim to simplify the setup for **local** testing and development, i.e. on a developer's machine.

Just to be clear, this setup is not at all intended for a production deployment and it is not suitable for that either.
However, it could serve as an example to get you started, if you want to create your own production setup.


The goal here is different though.

1. The openEO backend is built to run on Linux servers or containers but developers may be working on Windows or Mac, so we want to make it easier for them to get a container up and running.
2. Having a working example of a minimal container serves as documentation and it is a good starting point to build more complex, tailored solutions.


Sub goals toward a full example setup:

- [x] Be able to run test suite (pytest) in Docker.
- [ ] Be able to run a batch job for testing.
- [ ] Running backend as local docker container, namely be able to use the REST API.


That said, what this is specifically ***not*** a goal for this setup:

- It does not do scaling or high availability. That is something for Kubernetes, Docker Swarm or alike.
- This is not a secure, hardened configuration and should only be run for local development accessible on localhost only.


## Dependencies

> **TL;DR:** Optional dependencies if you want to use the makefile:
>
> - GNU make
> - grep and sed, on Linux or in git-bash
> - ripgrep, if you are on Windows and want to work in Powershell


Besides Docker it is recommended you have GNU make installed.
Using make is entirely optional, but some of the steps get pretty repetitive when you are developing. The makefile gives you a more automated way run common build and test commands, and makes it easier to run the different build steps in the correct order.


We are also working on a simpler development setup using docker-compose, because that is a bit more developer-friendly. Then you can get up and running with just a `docker compose up`. That is still a work in progress at present. (d.d. 2023-09-12)

That said, one benefit of the makefile is that it also serves as an example of the docker commands, and in what order you should run these commands.

### Dependencies for the Makefile

The provided makefile should be compatible with GNU make and run on both Linux and Windows.
(And let's keep it compatible, please)

With Windows the simplest option is to run it inside `git-bash`, though it works also in Powershell, with minor features missing out of the box.
I.e. the command `make help` would require extra tools to be installed to work in Powershell.

If you are on Windows, here are some of the easiest ways to get GNU make:

1) Download and run the GNUWin32 installer for GNU make:

[Download page dor GNUWin32 Make](https://gnuwin32.sourceforge.net/packages/make.htm)

2) Install via winget

```shell
winget install GnuWin32.Make
```

3) Install via scoop

```shell
scoop install make
```

### Optional Dependencies for `make help`

You can display the make targets with a brief help description by running `make help`.
However, the `make help` target does need a few extra commands for it to work.

Showing help is just something to make your life easier, so it won't block you if this doesn't work.

On Linux (or in git-bash) it relies on the `grep` and `sed` commands` to find the "docstrings" (by lack of a better term) of the makefiles.

On Windows in Powershell these won't be available easily but a simple solution is to install `ripgrep` via scoop.

```powershell
scoop install ripgrep
```

## How To Use The Makefile

The provided makefile should be compatible with GNU make and run on both Linux and Windows.
(And keep it compatible, please)

### Showing Help About What Target You Can Build/Run

You can show help about the different makefile targets.
The default make target is also "help" actually.

```shell
make help

# or with the path to Makefile:
make -f docker/Makefile all
```

### Regular build and test

To build a container and run the test suite, run the following command:

```shell
# in the project's sub directory `./docker`
make all
```

Or specifying the path to this makefile if you run make in the root of your git repo:

```shell
make -f docker/Makefile all
```

You don't need to run "build" every the time. In fact we try to avoid rebuilding
images and image layers as much as we can.

1. You should use the `-v` option in your `docker run` command to map your source code / working repository to the /src folder in the container.

That way the container sees your source code and it doesn't need to be rebuild every time you changed a line of code.
All our makefile targets that do a `docker run` use volume mapping.
For an example see the target `test` in docker\Makefile

2. The main point of this makefile was to be able to extract a pip requirements file from setup.py and install the python dependencies only when they change.

Therefor, if you have already done a "full build" once, and none of the
Python dependencies have changed in setup.py, then it should be enough
to build just the main image, like so:

```shell
make -f docker/Makefile build-main
```

### Make with `--dry-run`: See the Docker Command to Run

Make has a `--dry-run` option, or use the short option `-n`.
Then make will only print what commands it would run.

For example
```shell
make -f .\docker\Makefile --dry-run build
```

Would give output similar to this:

```log
docker buildx build -t openeo-geopyspark-driver-base:latest  --target base -f  C:/development/projects/VITO/codebases/openeo-geopyspark-driver
docker run  --rm -ti \
        -v C:/development/projects/VITO/codebases/openeo-geopyspark-driver:/src \
        openeo-geopyspark-driver-base:latest  pip-compile \
        -o ./requirements-docker.txt  \
        --verbose \
        --pip-args use-pep517 \
        -P rlguard-lib@git+https://github.com/sentinel-hub/rate-limiting-guard.git@master#subdirectory=lib \
        --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple \
        setup.py
docker build -t openeo-geopyspark-driver:latest -f  C:/development/projects/VITO/codebases/openeo-geopyspark-driver
docker run --rm -ti -v C:/development/projects/VITO/codebases/openeo-geopyspark-driver:/src  openeo-geopyspark-driver:latest python3 scripts/get-jars.py
```


## Configuration Variables

The Makefile allows to set a few configuration parameters, should you wish to.
Namely, the docker image name and the image tag and whether your development environment supports BuildKit or not.


There is a separate dockerfile for systems that don't have BuildKit: `minimal-no-buildkit.dockerfile`
The only way `minimal-no-buildkit.dockerfile` differs from` `minimal.dockerfile` is that the RUN commands don't use mounts for the cache of apt install and pip install commands because this requires buildkit.

See this part near the top of the makefile:

```makefile
# ==============================================================================
# Configuration variables
# ==============================================================================

docker_image_basename := openeo-geopyspark-driver
docker_tag := latest

# ...
buildkit_supported := 1
```

Do keep in mind that this example makefile is part of the git repo so it is
shared by everyone.

Because we want to allow local overrides without hindering other developers,
the makefile has the option to import a file called `makenv.env`, and in
`makenv.env` you can override the variables there if you need to.

The syntax of the makeenv.env file is "dotenv" or ".env" and its basic use is very simple:  `varname=value`

```env
# example makeenv.env contents:
docker_image_basename=my-own-container-name
docker_tag=py3.8
```

## Rationale for Some of the Choices

### Separate Docker Folder

A lot of people prefer to have their files for Docker and Docker Compose in the root of the project's git repo.
Further there are tools in IDE's that will generate Docker-related files in that location.

We don't want to interfere with those development workflows if you like to use those, so we have put this example in a dedicated subdirectory.
This makes our example a bit more complicated but it is better allow choice and leave this up to the individual developer.

### Reducing Docker Image Rebuild Times

We do mainly three things to reduce build times and package downloads:

1. Order the install commands so that the least frequently changing things are installed first, so Docker can leverage its layer caching.

2. For RUN statements: we use the option `--mount=type=cache,target=...` to avoid repeatedly downloading apt and pip packages when youy rebuild the Docker image. This requires BuildKit, but BuildKit is now available by default in recent versions of Docker. For old environments that don't have BuildKit: use `minimal-no-buildkit.dockerfile` instead of `minimal.dockerfile`.

3. For pip installs: first extract a requirements file, called  `docker/requirements-docker.txt`, using `pip-compile`. Then use that file to install only the Python dependencies, before copying our own source code and installing our own package in editable mode.


It can be argued that the step 3, extracting the requirements file, makes our current setup more complicated. However, if we would not install dependencies as a *separate* step then you end up re-installing all python packages on every rebuild, even when there are no changes to the dependencies themselves (i.e. setup.py is identical) This takes long and wastes time needlessly.

Further, these dependencies are specific to the docker image's environment, and that is important, especially when your host OS is Windows. You can not resolve dependencies on Windows and expect those resolved versions to work in a Linux environment because they may be different when some packages are OS-dependent. You have to resolve the full dependencies inside a similar Linux-based Docker image, or a similar Linux environment.


There are some possible alternative solutions but that may also require a change of development workflow. That needs a broader discussion and for now we just wanted to solve the problem at hand.
