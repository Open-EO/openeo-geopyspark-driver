import typing
import uuid
import geopyspark as gps
from enum import Enum
from pathlib import Path
from typing import Dict, Union

import pyspark
from pyspark.ml.classification import _JavaProbabilisticClassificationModel
from pyspark.ml.util import MLReadable, JavaMLWritable, JavaMLReader

from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
from pyspark.mllib.util import JavaSaveable

from openeo_driver.utils import generate_uuid


class EModelType(Enum):
    CatboostBinary = 0
    AppleCoreML = 1
    Cpp = 2
    Python = 3
    Json = 4
    Onnx = 5
    Pmml = 6
    CPUSnapshot = 7


_standard_py2java = pyspark.ml.common._py2java
_standard_java2py = pyspark.ml.common._java2py


def _py2java(sc, obj):
    """ Convert Python object into Java """
    if isinstance(obj, Enum):
        return getattr(
            getattr(
                sc._jvm.ru.yandex.catboost.spark.catboost4j_spark.core.src.native_impl,
                obj.__class__.__name__
            ),
            'swigToEnum'
        )(obj.value)
    return _standard_py2java(sc, obj)


class CatBoostMLReader(JavaMLReader):
    """
    (Private) Specialization of :py:class:`JavaMLReader` for CatBoost types
    """

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance.
        """
        return "ai.catboost.spark.CatBoostClassificationModel"


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

    @staticmethod
    def load_native_model(filename, file_format=EModelType.CatboostBinary):
        """
        Load the model from a local file.
        See https://catboost.ai/docs/concepts/python-reference_catboostclassifier_load_model.html
        for detailed parameters description
        """
        sc = gps.get_spark_context()
        java_model = sc._jvm.ai.catboost.spark.CatBoostClassificationModel.loadNativeModel(filename, _py2java(sc, file_format))
        return java_model


class GeopySparkCatBoostModel(DriverMlModel):

    def __init__(self, model: JavaSaveable):
        self._model = model

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = directory / "catboost_model.cbm"
        metadata = {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "type": "Feature",
            "id": generate_uuid(prefix="cb"),
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