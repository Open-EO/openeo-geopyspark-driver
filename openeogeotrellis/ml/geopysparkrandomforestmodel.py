import logging

import shutil
import typing
import uuid
from pathlib import Path
from typing import Dict, Union

from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
import geopyspark as gps
from pyspark.mllib.tree import RandomForestModel

from openeo_driver.utils import generate_unique_id
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.utils import download_s3_directory, to_s3_url

logger = logging.getLogger(__name__)

class GeopySparkRandomForestModel(DriverMlModel):

    def __init__(self, model: RandomForestModel):
        self._model = model

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = str(directory / "randomforest.model.tar.gz")
        metadata = {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "type": "Feature",
            "id": generate_unique_id(prefix="rf"),
            "collection": "collection-id",
            "bbox": [
                -179.999,
                -89.999,
                179.999,
                89.999
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            -179.999,
                            -89.999
                        ],
                        [
                            179.999,
                            -89.999
                        ],
                        [
                            179.999,
                            89.999
                        ],
                        [
                            -179.999,
                            89.999
                        ],
                        [
                            -179.999,
                            -89.999
                        ]
                    ]
                ]
            },
            'properties': {
                "datetime": None,
                "start_datetime": "1970-01-01T00:00:00Z",
                "end_datetime": "9999-12-31T23:59:59Z",
                "ml-model:type": "ml-model",
                "ml-model:learning_approach": "supervised",
                "ml-model:prediction_type": "classification",
                "ml-model:architecture": "random-forest",
                "ml-model:training-processor-type": "cpu",
                "ml-model:training-os": "linux",
            },
            'links': [],
            'assets': {
                'model': {
                    "href": model_path,
                    "type": "application/octet-stream",
                    "title": "org.apache.spark.mllib.tree.model.RandomForestModel",
                    "roles": ["ml-model:checkpoint"]
                }
            }
        }
        return metadata

    def write_assets(self, directory: Union[str, Path]) -> Dict[str, StacAsset]:
        """
        Save generated assets into a directory, return asset metadata.

        :return: STAC assets dictionary: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#assets
        """
        directory = Path(directory)
        if not directory.is_dir():
            directory = Path(directory).parent
        model_path = Path(directory) / "randomforest.model"

        # Save model to disk or s3 using spark.
        use_s3 = ConfigParams().is_kube_deploy
        spark_path = "file:" + str(model_path)
        if use_s3:
            spark_path = to_s3_url(model_path).replace("s3:", 's3a:')
        logging.info(f"Saving GeopySparkRandomForestModel to {spark_path}")
        self._model.save(gps.get_spark_context(), spark_path)

        # Archive the saved model.
        if use_s3 and not model_path.exists():
            logging.info(f"{model_path} does not exist, downloading it from s3 first.")
            download_s3_directory(to_s3_url(model_path), "/")
        logging.info(f"Archiving {model_path} to {model_path}.tar.gz")
        shutil.make_archive(base_name=str(model_path), format='gztar', root_dir=directory)
        logging.info(f"Removing original {model_path}")
        shutil.rmtree(model_path)
        model_path = Path(str(model_path) + '.tar.gz')
        logging.info(f"GeopySparkRandomForestModel stored as {model_path=}")
        return {model_path.name: {"href": str(model_path)}}

    def get_model(self) -> RandomForestModel:
        return self._model
    
    def load_native_model(sc, path) -> "GeopySparkRandomForestModel":
        model = RandomForestModel(RandomForestModel._load_java(sc=sc, path=path))
        return GeopySparkRandomForestModel(model)
