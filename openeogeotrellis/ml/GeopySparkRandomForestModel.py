import shutil
import typing
import uuid
from pathlib import Path
from typing import Dict, Union

from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
import geopyspark as gps
from pyspark.mllib.util import JavaSaveable

from openeo_driver.utils import generate_uuid


class GeopySparkRandomForestModel(DriverMlModel):

    def __init__(self, model: JavaSaveable):
        self._model = model

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = directory / "randomforest.model.tar.gz"
        metadata = {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
            ],
            "type": "Feature",
            "id": generate_uuid(prefix="rf"),
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
        directory = Path(directory).parent
        model_path = Path(directory) / "randomforest.model"
        self._model.save(gps.get_spark_context(), "file:" + str(model_path))
        shutil.make_archive(base_name=str(model_path), format='gztar', root_dir=directory)
        shutil.rmtree(model_path)
        model_path = Path(str(model_path) + '.tar.gz')
        return {model_path.name: {"href": str(model_path)}}

    def get_model(self):
        return self._model