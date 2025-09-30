# Run openEO Geotrellis Web Service on a single machine

## Limitations

1. Data will have to be loaded via load_stac, or you configure own collections
2. No batch job support, only synchronous
3. Resources are limited to single machine

## Steps

### Using podman

A shell script is available that runs the service in podman/docker.

Environment variables are taken from local_service.env, where they can be easily adapted.

```bash
docker pull vito-docker.artifactory.vgt.vito.be/openeo-base:latest

git clone https://github.com/Open-EO/openeo-geopyspark-driver.git
cd ./docker/local_openeo_server
sh local_service.sh
```

### Using Docker Compose



```shell
docker compose -f ./docker/local_openeo_server/docker-compose.yml -p dkrcmps-openeo-geopyspark-driver up -d web
```



### Testing the service

A local service doesn't do much due to lack of data, but you can try a very simple test graph:

```python
import openeo
connection = openeo.connect("http://localhost:8080/")
connection.authenticate_basic("openeo", "openeo")

process_graph = {"add": {"process_id": "add", "arguments": {"x": 3,"y": 5}, "result": True}}
result = connection.execute(process_graph)
print(result)
```
