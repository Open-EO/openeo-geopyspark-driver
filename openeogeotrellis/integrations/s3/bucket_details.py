from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.workspace.object_storage_workspace import ObjectStorageWorkspace


_BUCKET_TYPE_UNKNOWN = "UNKNOWN"
_BUCKET_TYPE_WORKSPACE = "WORKSPACE"
_REGION_UNKNOWN = "REGION_UNKNOWN"


@dataclass(frozen=True)
class BucketDetails:
    name: str
    region: str = _REGION_UNKNOWN
    type: str = _BUCKET_TYPE_UNKNOWN
    type_id: Optional[str] = None

    @classmethod
    def from_name(cls, bucket_name: str) -> BucketDetails:
        for ws_name, ws_details in get_backend_config().workspaces.items():
            if isinstance(ws_details, ObjectStorageWorkspace):
                if ws_details.bucket == bucket_name:
                    return cls(
                        name=bucket_name,
                        region=ws_details.region,
                        type=_BUCKET_TYPE_WORKSPACE,
                        type_id=ws_name
                    )

        return cls(
            name=bucket_name,
        )


def is_workspace_bucket(bucket_details: BucketDetails) -> bool:
    return bucket_details.type == _BUCKET_TYPE_WORKSPACE
