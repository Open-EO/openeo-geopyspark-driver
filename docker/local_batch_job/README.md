# Run openEO from Docker file (experimental)

This tool allows to run simple openEO processes. External collections can be loaded with load_stac.
prerequisites: Linux with Docker already installed.

- clone this repository, or download just the `local_batch_job` folder
- Build the image first with `sudo docker build -t openeo_docker_local .`
- Run graph with `./local_batch_job.sh path/to/process_graph.json`
- The output files will be written to the same folder as process_graph.json

## Use local STAC catalogs
The easiest is to host the local stac catlog in a local web-server. Giving fast access from within the Docker container.

For example:
```bash
curl -L -O https://artifactory.vgt.vito.be/artifactory/testdata-public/CROP_MASK_COLLECTION.zip
unzip CROP_MASK_COLLECTION.zip
cd CROP_MASK_COLLECTION
pip install rangehttpserver
python3 -m RangeHTTPServer -b localhost 8000
```

No you can use this URL in load_stac: http://localhost:8000/CROP_MASK_STAC/collection.json
For example:
```json
{
  "process_graph": {
    "load1": {
      "arguments": {
        "spatial_extent": {
          "west": 27,
          "south": -27,
          "east": 30,
          "north": -26
        },
        "temporal_extent": [
          "2023-06-01T00:00:00Z",
          "2023-06-09T00:00:00Z"
        ],
        "url": "http://localhost:8000/CROP_MASK_STAC/collection.json"
      },
      "process_id": "load_stac",
      "result": true
    }
  }
}
```
