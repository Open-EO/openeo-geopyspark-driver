import logging
import shutil
import typing
import uuid
from enum import Enum
from pathlib import Path
from typing import Dict, Union

import geopyspark as gps
import pyspark
from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
from openeo_driver.utils import generate_unique_id
from openeogeotrellis.configparams import ConfigParams
from openeogeotrellis.ml.catboost_spark import CatBoostClassificationModel
from openeogeotrellis.utils import download_s3_directory, to_s3_url
from pyspark.ml.classification import _JavaProbabilisticClassificationModel
from pyspark.ml.util import JavaMLReader, JavaMLWritable, MLReadable
from pyspark.mllib.util import JavaSaveable


class GeopySparkCatBoostModel(DriverMlModel):

    def __init__(self, model: CatBoostClassificationModel):
        self._model = model

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = str(directory / "catboost_model.cbm.tar.gz")
        metadata = {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "type": "Feature",
            "id": generate_unique_id(prefix="cb"),
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
                "ml-model:architecture": "catboost",
                "ml-model:training-processor-type": "cpu",
                "ml-model:training-os": "linux",
            },
            'links': [],
            'assets': {
                'model': {
                    "href": model_path,
                    "type": "application/octet-stream",
                    "title": "ai.catboost.spark.CatBoostClassificationModel",
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
        model_path = Path(directory) / "catboost_model.cbm"

        # Save model to disk.
        # TODO: We might require s3 support here.
        spark_path = "file:" + str(model_path)
        logging.info(f"Saving GeopySparkCatboostModel to {spark_path}")
        self._model.save(spark_path)

        # Archive the saved model.
        logging.info(f"Archiving {model_path} to {model_path}.tar.gz")
        shutil.make_archive(base_name=str(model_path), format='gztar', root_dir=directory)
        logging.info(f"Removing original {model_path}")
        shutil.rmtree(model_path)
        model_path = Path(str(model_path) + '.tar.gz')
        logging.info(f"GeopySparkCatboostModel stored as {model_path=}")
        return {model_path.name: {"href": str(model_path)}}

    def get_model(self) -> CatBoostClassificationModel:
        return self._model
