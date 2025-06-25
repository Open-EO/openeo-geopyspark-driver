# Run openEO Geotrellis Web Service on a single machine

## Limitations

1. Data will have to be loaded via load_stac, or you configure own collections
2. No batch job support, only synchronous
3. Resources are limited to single machine

## Steps

### Using podman

A shell script is available that runs the service in podman.

Environment variables are taken from local_service.env, where they can be easily adapted.

```bash
docker pull vito-docker.artifactory.vgt.vito.be/openeo-base:latest
local_service.sh
```

### Using Docker Compose



```shell
docker compose -f ./docker/local_openeo_server/docker-compose.yml -p dkrcmps-openeo-geopyspark-driver up -d web
```



### Testing the service

A local service doesn't do much due to lack of data, but you can try a very simple test graph:

```python
import openeo
c = openeo.connect("http://localhost:8080/openeo/1.2").authenticate_oidc()

result = c.datacube_from_flat_graph({"add":{"process_id":"add","arguments":{"x":3,"y":5},"result":True}}).execute()
print(result)
```
