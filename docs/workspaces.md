# Configuring an OpenEO workspace

This describes the configuration of an OpenEO workspace, to be used with the `export_workspace` process.

## Context

OpenEO supports the [`export_workspace`](https://github.com/Open-EO/openeo-python-driver/blob/master/openeo_driver/specs/openeo-processes/experimental/export_workspace.json)
process to export (copy) batch job result assets to a particular user workspace, addressed by a workspace ID.

The `merge` argument of `export_workspace` (a path) determines where in the workspace the assets end up.
`export_workspace` will also create a STAC Collection that references these assets.

Currently, there is no API yet to let a user define a workspace; instead they have to be added as part of the
`GpsBackendConfig` as described in [configuration.md](configuration.md).

## Workspace types

There are currently three types of workspaces:
 - `openeo_driver.workspace.DiskWorkspace`: exports assets and writes a STAC Collection to disk;
 - `openeogeotrellis.workspace.object_storage_workspace.ObjectStorageWorkspace`: exports assets and writes a STAC
Collection to object storage;
 - `openeogeotrellis.workspace.stac_api_workspace.StacApiWorkspace`: creates a STAC Collection in a particular STAC API;
the destination of the assets is configurable.
