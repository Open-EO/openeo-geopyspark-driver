# CWL based openEO processing using Calrissian

This document describes how CWL based processing is implemented
in the openEO GeoPySpark driver when running on a Kubernetes cluster.

Read also the user facing documentation: [udf-eoap-cwl.md](./udf-eoap-cwl.md)

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
