# How to use the docker setup for development

## Goal: for development use only, or as an example for your own setup

The dockerfile and build scripts that you find in here aim to simplify the setup for **local** testing and development, i.e. on a developer's machine.

Just to be clear, this setup is not at all intended for a production deployment and it is not suiteable for that either.
However, it could serve as an example to get you started, if you want to create your own production setup.


The goal here is different though.

1. The openEO backend is built to run on Linux servers or containers but developers may be working on Windows or Mac, so we want to make it easier for them to get a container up and running.
2. Having a working example of a minimal container is a good documentation and starting point to build more complex, tailored solutions.  


Sub goals toward a full example setup:

- [x] Be able to run test suite (pytest) in Docker.
- [ ] Be able to run a batch job for testing.
- [ ] Running backend as local docker container, namely be able to use the REST API.


That said, what this is specically ***not*** a goal for this setup:

- It does not do scaling or high availablity. That is something for Kubernetes, Docker Swarm or alike.
- This is not a secure, hardened configuration and should only be run for local development accessible on localhost only.


## Dependencies

> **TL;DR:** Optional dependencies if you want to use the makefile:
>
> - GNU make
> - grep and sed, on Linux or in git-bash
> - ripgrep, if you are on Windows and want to work in Powershell


Besides Docker you will need GNU make, if you want a more automated way run common build and test commands.
Using make is optional, but some of the steps get pretty repetitive when you are developing.

The provided makefile should be compatible with GNU make and run on both Linux and Windows.
(And let's keep it compatible, please)

If anything, the makefile will give you examples of the docker build and run commands you need,
and the order of the build steps.

With Windows the simplest option is to run it in git bash, though it works also in Powershell, with minor features missing out of the box.
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

The `make help` target does need a few extra commands for it to work. 

Showing help just something to make your life easier, but it won't block you if this doesn't work.
On Linux (or in git-bash) it relies on grep and sed to find the "doctrings" (by lack of a better term) of the makefiles.
On Windows in Powershell these won't be available easily but a simple solution is to install ripgrep via scoop 

```powershell
scoop install ripgrep
```

## How to Use the Makefile

The provided makefile should be compatible with GNU make and run on both Linux and Windows.
(And keep it compatible, please)

To build a container and run the test suite, run the following command:

```shell
# in the project's sub directory `./docker`
make all
```

Or specifying the path to this makefile if you run make in the root of your git repo:

```shell
make -f docker/Makefile all
```

You can show help about the different makefile targets.
The default make target is also "help" actually.

```shell
make help

# or with the path to Makefile:
make -f docker/Makefile all
```


## Configuration variables

The Makefile allows to set a few confiration parameters, should you wish to.
Namely, the docker image name and the image tag.

See this part near the top of the makefile:

```makefile
# ==============================================================================
# Configuration variables
# ==============================================================================

docker_image_basename := openeo-geopyspark-driver
docker_tag := latest
```

Do keep in mind that this example makefile is part of the git repo so it is
shared by everyone.

Because we want to allow local overrides without hindering other developers,
the makefile has the option to import a file called `makenv.env`, and in 
`makenv.env` you can override the variables there if you need to.

The syntax of the makenenv.env file is "dotenv" or ".env" and its basic 
use is very simple:  `varname=value`

```env
# example makeenv.env contents:
docker_image_basename=my-own-container-name
docker_tag=py3.8
```
