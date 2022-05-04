import typing
import uuid
from pathlib import Path
from typing import Dict, Union

from pyspark.ml.classification import _JavaProbabilisticClassificationModel
from pyspark.ml.util import MLReadable, JavaMLWritable, JavaMLReader

from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
from pyspark.mllib.util import JavaSaveable


class CatBoostMLReader(JavaMLReader):
    """
    (Private) Specialization of :py:class:`JavaMLReader` for CatBoost types
    """

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance.
        """
        java_package = clazz.__module__.replace("catboost_spark.core", "ai.catboost.spark")
        print("CatBoostMLReader._java_loader_class. ", java_package + "." + clazz.__name__)
        return "ai.catboost.spark.CatBoostClassificationModel"
        # return java_package + "." + clazz.__name__


class CatBoostClassificationModel(_JavaProbabilisticClassificationModel, MLReadable, JavaMLWritable):
    """
    Simple wrapper around the java CatBoostClassificationModel implementation.
    """
    def __init__(self, java_model=None):
        super(CatBoostClassificationModel, self).__init__(java_model)

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return CatBoostMLReader(cls)

    @staticmethod
    def _from_java(java_model):
        return CatBoostClassificationModel(java_model)


class GeopySparkCatBoostModel(DriverMlModel):

    def __init__(self, model: JavaSaveable):
        self._model = model

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = directory / "catboost_model.tar.gz"
        metadata = {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "type": "Feature",
            "id": str(uuid.uuid4()),
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
        raise NotImplementedError

    def get_model(self):
        raise NotImplementedError