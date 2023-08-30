from typing import Optional

import py4j.java_gateway

from openeogeotrellis.config import get_backend_config
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.sentinel_hub import OG_BATCH_RESULTS_BUCKET
from openeogeotrellis.utils import get_jvm


class SentinelHubBatchProcessing:
    @staticmethod
    def get_batch_processing_service(
        endpoint: str = "https://services.sentinel-hub.com",
        bucket_name: str = OG_BATCH_RESULTS_BUCKET,
        sentinel_hub_client_id: Optional[str] = None,
        sentinel_hub_client_secret: Optional[str] = None,
        sentinel_hub_client_alias: str = "default",
        jvm: Optional[py4j.java_gateway.JVMView] = None,
    ):
        """
        Helper to build a `org.openeo.geotrellissentinelhub.BatchProcessingService`
        and allow mocking for testing
        """
        jvm = jvm or get_jvm()
        return jvm.org.openeo.geotrellissentinelhub.BatchProcessingService(
            endpoint,
            bucket_name,
            sentinel_hub_client_id,
            sentinel_hub_client_secret,
            ",".join(get_backend_config().zookeeper_nodes),
            # TODO: get path prefix from config?
            f"/openeo/rlguard/access_token_{sentinel_hub_client_alias}",
        )
