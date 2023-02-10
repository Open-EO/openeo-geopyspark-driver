from typing import Optional

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
    ):
        """
        Helper to build a `org.openeo.geotrellissentinelhub.BatchProcessingService`
        and allow mocking for testing
        """
        return get_jvm().org.openeo.geotrellissentinelhub.BatchProcessingService(
            endpoint,
            bucket_name,
            sentinel_hub_client_id,
            sentinel_hub_client_secret,
            ",".join(ConfigParams().zookeepernodes),
            # TODO: get path prefix from config?
            f"/openeo/rlguard/access_token_{sentinel_hub_client_alias}",
        )
