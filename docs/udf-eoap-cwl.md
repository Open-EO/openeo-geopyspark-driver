# UDF using CWL

User Defined Process using Common Workflow Language.

It is now possible to run CWL workflows inside the `run_udf` process.
For the UDF parameter, you can use a CWL workflow definition as a string, or use an URL that points to a CWL workflow
definition.

Here is an example how to use this in Python:

```python
datacube = connection.datacube_from_process(
    "run_udf",
    data=None,
    udf=cwl,
    runtime="EOAP-CWL",
    context={"example_key": "example_value"},
)
```

## Calrissian

The CWL is executed using [Calrissian](https://duke-gcb.github.io/calrissian/).
> Calrissian is a CWL implementation designed to run inside a Kubernetes cluster.
> Its goal is to be highly efficient and scalable, taking advantage of high capacity clusters to run many steps in
> parallel.

> Calrissian leverages [cwltool](https://github.com/common-workflow-language/cwltool) heavily and most conformance tests
> for CWL v1.0.

## S3 access

Your CWL code will receive temporary S3 credentials to access the `eodata` bucket.
Those credentials can be accessed through the following environment variables:

- `AWS_ENDPOINT_URL_S3`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

They won't work outside the cluster, and are only temporary valid.

## Docker images

Right now, only whitelisted docker images can be used in the cluster. Contact us to get your image prefix whitelisted.

## Debugging locally

You might want to test your CWL workflow locally before running it on the cluster.
To do this, you can use `cwltool` locally.
You might need to provide your own S3 credentials. You can request them
here: https://documentation.dataspace.copernicus.eu/APIs/S3.html

```bash
cwltool
  --tmpdir-prefix=$HOME/tmp/
  --force-docker-pull
  --leave-container
  --leave-tmpdir
  --no-read-only
  --parallel
  --preserve-environment=AWS_ENDPOINT_URL_S3
  --preserve-environment=AWS_ACCESS_KEY_ID
  --preserve-environment=AWS_SECRET_ACCESS_KEY
  example_workflow.cwl example_parameters.json
```

## Examples

Some workflows are being developed that use CWL. For example:

- https://github.com/cloudinsar/s1-workflows
- https://github.com/bcdev/apex-force-openeo
