# How to use the docker setup for development

The dockerfile and build scripts that you find in here aim to simplify the setup for **local** testing and development, i.e. on a developer's machine.

Just to be clear, this setup is not at all intended for a production deployment and it is not suiteable for that either.
However, it could serve as an example to get you started, if you want to create your own production setup.


The goal here is different though.

1. The openEO backend is built to run on Linux servers or containers but developers may be working on Windows or Mac, so we want to make it easier for them to get a container up and running.
2. Having a working example of a minimal container is a good documentation and starting point to build more complex, tailored solutions.  


Goals:

- [x] Be able to run test suite (pytest) in Docker.
- [ ] Be able to run a batch job for testing.
- [ ] Running backend as local docker container, namely be able to use the REST API.


That said, what this is specically **not** a goal for this setup:

- It does not do scaling or high availablity. That is something for Kubernetes, Docker Swarm or alike.
- This is not a secure, hardened configuration and should only be run for local development accessible on localhost only.


## Dependencies

Besides Docker you will need GNU make, if you want a more automated way run common commands or run several build steps.
This is optional, but some of the steps get pretty repetitive when you are developing.

The provided makefile should be compatible with GNU make and run on both Linux and Windows.

With Windows the simplest option is to run it in git bash, though it works also in Powershell, with minor features missing.
I.e. `make help`  would require extra tools to be installed to work in Powershell.

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

## How to Use the Makefile

To build a container and run the test suite, run the following command:

```shell
make all
```

Or specifying the path to this makefile:

```shell
make -f docker/Makefile all
```

Get help about the different makefile targets
The default make target is also "help" actually.

```
make help

# or
make -f docker/Makefile all
```
