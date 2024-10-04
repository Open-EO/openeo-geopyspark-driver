# Run openEO Geotrellis locally using Docker

This tool allows to run simple openEO processes. External collections can be loaded with load_stac.

Note that the openEO Geotrellis backend was designed primarily to run on a distributed processing cluster. 
The purpose of this tool is for instance to allow faster iterations on small test datasets, without depending on an online deployment.
It should also work for larger workloads, but this may require sufficient IT skills or insight into the workings of a Spark application.
For users that require support, we recommend using one of the online deployments. 

Prerequisites: Docker already installed. Python with the openEO client installed.
Note, you can refer to local stac collections by file path on Linux, but not on Windows.

- Clone / download this repository
- Build the image with `cd openeo-geopyspark-driver && sudo docker build -t openeo_docker_local . -f docker/local_batch_job/Dockerfile`
- Run graph with `./local_batch_job.sh path/to/process_graph.json`
- The output files will be written to the same folder as process_graph.json

## Example:

[local_batch_job_example.py](./local_batch_job_example.py)  runs a small openEO process in a local docker container.
Here you can use load_stac on catalogs that are hosted locally. Removing a dependency on the internet and allowing for faster processing.


