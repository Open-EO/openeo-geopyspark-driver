import os
from pprint import pformat
from typing import Optional


class ConfigParams:

    def __init__(self, env=os.environ):
        self.zookeepernodes = env.get(
            "ZOOKEEPERNODES",
            'epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181'
        ).split(',')

        self.batch_jobs_zookeeper_root_path = env.get("BATCH_JOBS_ZOOKEEPER_ROOT_PATH", "/openeo/jobs")
        self.async_task_handler_environment = env.get("ASYNC_TASK_HANDLER_ENV")
        self.cache_shub_batch_results = ConfigParams._as_boolean(env.get("CACHE_SHUB_BATCH_RESULTS"))

        # Are we running in a unittest or continuous integration context?
        self.is_ci_context = any(v in env for v in ['PYTEST_CURRENT_TEST', 'PYTEST_CONFIGURE'])

        # TODO: can we avoid using env variables?
        self.layer_catalog_metadata_files = env.get("OPENEO_CATALOG_FILES", "layercatalog.json").split(",")

        self.default_opensearch_endpoint = env.get("OPENSEARCH_ENDPOINT", "https://services.terrascope.be/catalogue")

        self.is_kube_deploy = env.get("KUBE", False)

        self.s1backscatter_elev_geoid = env.get("OPENEO_S1BACKSCATTER_ELEV_GEOID")

    def __str__(self) -> str:
        return pformat(vars(self))

    @staticmethod
    def _as_boolean(envar_value: Optional[str]) -> bool:
        return envar_value is not None and envar_value.lower() == "true"
