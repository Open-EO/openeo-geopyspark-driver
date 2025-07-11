# Configuring an OpenEO Workspace

This describes the configuration of an OpenEO workspace, to be used with the `export_workspace` process.

## Context

OpenEO supports the [`export_workspace`](https://github.com/Open-EO/openeo-python-driver/blob/master/openeo_driver/specs/openeo-processes/experimental/export_workspace.json)
process to export (copy) batch job result assets to a particular user workspace, addressed by a workspace ID.

The `merge` argument of `export_workspace` (a path) determines where in the workspace the assets end up.
`export_workspace` will also create a STAC Collection that references these assets.

Currently, there is no API yet to let a user define a workspace; instead they have to be added as part of the
`GpsBackendConfig` as described in [configuration.md](configuration.md). For example, a `DiskWorkspace` that is referenced by
workspace ID `disk_workspace_id` might look like this:

```python
from openeo_driver.workspace import DiskWorkspace

config = GpsBackendConfig(
    workspaces={
        "disk_workspace_id": DiskWorkspace(root_directory=Path("/path/to/workspace/root")),
    },
)
```

## Workspace Types

There are currently three types of workspaces:
 - `DiskWorkspace`: exports assets and writes a STAC Collection to disk;
 - `ObjectStorageWorkspace`: exports assets and writes a STAC
Collection to object storage;
 - `StacApiWorkspace`: merges a STAC Collection in a particular STAC API;
the destination of the assets is configurable.

### Diskworkspace

**TODO**: document `openeo_driver.workspace.DiskWorkspace`

### ObjectStorageWorkspace

**TODO**: document `openeogeotrellis.workspace.object_storage_workspace.ObjectStorageWorkspace`

### StacApiWorkspace

This is an instance of `openeogeotrellis.workspace.stac_api_workspace.StacApiWorkspace`.

This workspace implementation will merge a STAC Collection into a new or existing STAC API; therefore, it requires the
root URL of this STAC API (`root_url`).

Updating a STAC API typically requires authentication. A call to `get_access_token` should return a
valid access token; it will be included in requests towards the STAC API (the `Authorization` header). Some STAC APIs
allow access control (read/write) at the Collection level; this could involve passing additional properties in the
Collection document upon creation of this Collection: `additional_collection_properties`.

The `export_workspace` process typically exports (copies) assets as well. In the case of `StacApiWorkspace`, how this is
accomplished is configurable by means the `export_asset` argument. This method is called for every asset that is to be
exported and takes a couple of arguments itself:
- the asset object as a `pystac.asset.Asset`;
- the `merge` argument passed to `export_workspace`: this allows the client to write assets to different locations
within the workspace;
- a relative asset path to properly support `filepath_per_band`;
- a flag that determines that exported assets are to be moved (`True`) or simply copied (`False`).

To be clear: `export_asset` receives (a URI to) the original asset and should take care of actually copying this asset
to the workspace. It should also return a URI to this asset as it appears in the workspace so it can be included as a
link in the original [OpenEO batch job results document](https://openeo.org/documentation/1.0/developers/api/reference.html#tag/Batch-Jobs/operation/list-results);
in this case, `asset_alternate_id` is used as a key for an `alternate` `href` to this asset.

Because this requires a lot of ceremony and a pattern emerged from the currently configured `StacApiWorkspace`s,
the helper function [`openeogeotrellis.workspace.vito_stac_api_workspace`](https://github.com/Open-EO/openeo-geopyspark-driver/blob/master/openeogeotrellis/workspace/helpers.py)
was introduced to make configuration easier if:
- the STAC API uses OIDC client credentials for access and,
- assets are copied to object storage.

Example:

```python
from openeogeotrellis.workspace import vito_stac_api_workspace

config = GpsBackendConfig(
    workspaces={
        "stac_api_workspace_id": vito_stac_api_workspace(
            root_url="https://stac.test",
            oidc_issuer="https://sso.test/auth/realms/test",
            oidc_client_id="client_id",
            oidc_client_secret="cl13nt_s3cr3t",
            asset_bucket="bucket_name",
            additional_collection_properties={
                "_auth": {
                    "read": ["anonymous"],
                    "write": ["admin", "editor"],
                }
            }
        ),
    },
)
```

## Job Options

**TODO**: `remove-exported-assets`

**TODO**: `export-workspace-enable-merge`
