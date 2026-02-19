# UDF using CWL

User-Defined Functions using Common Workflow Language.

It is now possible to run CWL workflows inside the `run_udf` process.
For the UDF parameter, you can use a CWL workflow definition as a string or use a URL that points to a CWL workflow
definition. It has been tested with CWL v1.0 and v1.2 other versions probably also work out of the box.

Here is an example of how to use this in Python:

```python
cwl = """
cwlVersion: v1.0
class: CommandLineTool
# This uses a very simple image that packages some JSON and TIFF files and copies them to the output directory.
requirements:
  - class: DockerRequirement
    dockerPull: vito-docker.artifactory.vgt.vito.be/openeo-geopyspark-driver-example-stac-catalog:1.8
baseCommand: ["sh", "-c", "cp /data/* ."]
inputs: []
outputs:
  output:
    type: Directory
    outputBinding:
      glob: .
"""
# or: cwl = "https://example.com/dummy_stac.cwl"
datacube = connection.datacube_from_process(
    "run_udf",
    data=None,
    udf=cwl,
    runtime="EOAP-CWL",
    context={"example_key": "example_value"},
)
```

## Context

[CWL (Common Workflow Language)](https://www.commonwl.org) is an open standard
for describing how to run (command line) tools and connect them to create workflows.
While CWL and openEO are a bit competing due to this conceptual overlap,
there is demand to run existing or new CWL workflows as part of a larger openEO processing chain.

The CWL is executed using [Calrissian](https://duke-gcb.github.io/calrissian/).
> Calrissian is a CWL implementation designed to run inside a Kubernetes cluster.
> Its goal is to be highly efficient and scalable, taking advantage of high capacity clusters to run many steps in
> parallel.

> Calrissian leverages [cwltool](https://github.com/common-workflow-language/cwltool) heavily and most conformance tests
> for CWL v1.0.

## CWL Output

The CWL workflow should output a STAC collection, with the root being `collection.json`. The STAC will then be read by
openEO with an internal call to `load_stac` and the output of the `run_udf` process will be a datacube. This catalog can
be self-contained, or refer to publicly available data.
For STAC best practices, refer
to: [stac_best_practices.md](https://github.com/EOEPCA/datacube-access/blob/main/best_practices/stac_best_practices.md)

## Resource usage

We recommend specifying resource limits for your CWL workflow steps. Not specifying it might allocate all your steps on
the same node in the cluster, which might lead to out-of-memory errors. You can specify the memory limits in the CWL
workflow definition using the `ResourceRequirement` field. For the safest scheduling, keep `ramMin` and `ramMax` the
same value.

An example that uses 4GB:

```yaml
requirements:
  ResourceRequirement:
    ramMin: 4000
    ramMax: 4000
    coresMin: 2
    coresMax: 7
```

## Logging

OpenEO tries to capture the logs of the CWL workflow and make them available in the openEO logs.
To help this, it is best to avoid capturing stdout/stderr. More details
here: https://eoap.github.io/logging-and-redirection/

Also, logging as JSON with a timestamp would allow the editor to show the logs in the correct order. An example of how
to do this in Python is shown
here: [s1-workflows](https://github.com/cloudinsar/s1-workflows/blob/312ef82fe5ac929dece30950abdbc618025d6c9a/sar/utils/workflow_utils.py#L29-L47)

## Network access

For later versions of CWL, you might need to enable network access:

```yaml
requirements:
  - class: NetworkAccess
    networkAccess: true
```

## Examples

Here are some workflows being developed that use CWL that can be used as examples:

- https://github.com/cloudinsar/s1-workflows
- https://github.com/bcdev/apex-force-openeo
