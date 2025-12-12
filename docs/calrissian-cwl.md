# CWL based openEO processing using Calrissian

This document describes how CWL based processing is implemented
in the openEO GeoPySpark driver when running on a Kubernetes cluster.

## Background and terminology

[CWL (Common Workflow Language)](https://www.commonwl.org) is an open standard
for describing how to run (command line) tools and connect them to create workflows.
While CWL and openEO are a bit competing due to this conceptual overlap,
there is demand to run existing or new CWL workflows as part of a larger openEO processing chain.
This is comparable to UDFs which allow to run (Python) scripts as part of an openEO workflow.

[Calrissian](https://duke-gcb.github.io/calrissian/) is a CWL implementation
designed to run inside a Kubernetes cluster.


## Kubernetes minikube setup

First, make sure `minikube` is installed: https://minikube.sigs.k8s.io/docs/start
Then run [deployment/setup_calrissian_cwl_on_minikube.sh](deployment/setup_calrissian_cwl_on_minikube.sh)

The setup is based on the [Calrissian cluster configuration docs](https://duke-gcb.github.io/calrissian/cluster-configuration/).


## Further openeo-geopyspark-driver configuration

Further configuration of the openeo-geopyspark-driver application
is done through the `calrissian_config` field of `GpsBackendConfig`
(also see the general [configuration docs](./configuration.md)).
This field expects (unless no Calrissian integration is necessary) a
`CalrissianConfig` sub-configuration object
(defined at `openeogeotrellis.config.integrations.calrissian_config`),
which allows to configure various aspects of the Calrissian integration.
