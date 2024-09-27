# Run openEO from Docker file (experimental)

This tool allows to run simple openEO processes. External collections can be loaded with load_stac.
prerequisites: Linux with Docker already installed.

- clone this repository, or download just the `local_batch_job` folder
- Build the image first with `sudo docker build -t openeo_docker_local .`
- Run graph with `local_batch_job.sh path/to/process_graph.json`
- The output files will be written to the same folder as process_graph.json
