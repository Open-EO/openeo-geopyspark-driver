# Run openEO Geotrellis Batch job on a single machine

The Geotrellis openEO batch jobs can run as a single process on one machine. This is useful for small-scale jobs
that run on one large machine, or for development and testing purposes. It avoids running the full openEO web application,
which drastically simplifies the setup.

The benefit of this setup is that you do not require to manage a distributed processing cluster using Kubernetes or YARN.
You also do not have to deal with dependencies required by the web application, and can easily submit single batch jobs.
The drawback is that scaling is limited to the resources of a single machine, which can still be quite significant, but is not comparable to a distributed cluster.

## Prerequisites

For testing and debugging, a machine with Docker and a couple of GB of RAM is sufficient. For production use, we recommend
at least 40GB of free RAM for very small deployments, with capacity of running 1 non-trivial batch job at time. 

## The `local_batch_job` tool

This tool allows to run simple openEO processes. Collections can be loaded with `load_stac`.


Prerequisites: Docker already installed. Python with the openEO client installed.
If your version is older than 0.33.0, upgrade with the following command: `pip install openeo --upgrade`
Note, you can refer to local stac collections by file path on Linux, but not on Windows.

- Clone / download this repository

- Build the image (from the project root):
    - First, make sure you have a recent enough base image, by pulling it:

          docker pull vito-docker.artifactory.vgt.vito.be/openeo-base:latest

    - Then build the image

          docker build -t openeo_docker_local . -f docker/local_batch_job/Dockerfile

- Execute a batch job from a process graph JSON file locally with

      ./local_batch_job path/to/process_graph.json folder/that/contains/local/stac/catalogs/

- The output files will be written to the same folder as process_graph.json

## Example:

[local_batch_job_example.py](./local_batch_job_example.py)  runs a small openEO process in a local docker container.
It shows you can use load_stac on static STAC catalogs that are hosted locally. 
