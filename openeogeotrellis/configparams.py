import os


class ConfigParams:

    def __init__(self, env=os.environ):
        self.zookeepernodes = env.get(
            "ZOOKEEPERNODES",
            'epod-master1.vgt.vito.be:2181,epod-master2.vgt.vito.be:2181,epod-master3.vgt.vito.be:2181'
        ).split(',')

        # Are we running in a unittest or continuous integration context?
        self.is_ci_context = any(v in env for v in ['TRAVIS', 'PYTEST_CURRENT_TEST', 'PYTEST_CONFIGURE'])

        # TODO: can we avoid using env variables?
        self.layer_catalog_metadata_files = env.get("OPENEO_CATALOG_FILES", "layercatalog.json").split(",")

        self.opensearch_endpoint = env.get("OSCARS_ENDPOINT", "https://services.terrascope.be/catalogue")
