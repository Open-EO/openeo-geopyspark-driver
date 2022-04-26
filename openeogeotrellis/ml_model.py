import typing
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Union

from openeo_driver.datacube import DriverMlModel
from openeo_driver.datastructs import StacAsset
from openeo_driver.errors import ProcessParameterInvalidException
from openeo_driver.save_result import AggregatePolygonSpatialResult
import geopyspark as gps
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import JavaSaveable


class GeopySparkMLModel(DriverMlModel):

    def __init__(self, model: JavaSaveable):
        self._model = model
        self._filename = "randomforest.model"

    def get_model_metadata(self, directory: Union[str, Path]) -> Dict[str, typing.Any]:
        # This metadata will be written to job_metadata.json.
        # It will then be used to dynamically generate ml_model_metadata.json.
        directory = Path(directory).parent
        model_path = directory / self._filename
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
        model_path = Path(directory) / self._filename
        self._model.save(gps.get_spark_context(), "file:" + str(model_path))

        return {model_path.name: {"href": str(model_path)}}

    def get_model(self):
        return self._model

    def save_ml_model(self, directory: str) -> Dict:
        return self.write_assets(directory)


class AggregateSpatialVectorCube(AggregatePolygonSpatialResult):
    """
    TODO: This is a temporary class until vector cubes are fully implemented.
    """

    def fit_class_random_forest(
            self,
            target: dict,
            num_trees: int = 100,
            max_variables: Optional[Union[int, str]] = None,
            seed: Optional[int] = None,
    ) -> 'GeopySparkMLModel':
        """
        @param self (predictors):
        Vector cube with shape: (1, #geometries, #bands)
        A feature vector will be calculated per geometry. E.g. for geometry i:
        Bands: ["JAN_B01", "JAN_B02", "FEB_B01", "FEB_B02"]
        Values: [4.5, 2.2, 3.1, 4.4]
        Feature vector: [4.5, 2.2, 3.1, 4.4]

        @param target:
        Vector cube with shape: (#geometries)
        A label will be calculated per geometry. Labels are categorical, represented by an int.
        The total number of classes to train on = max label.
        E.g. for geometry i:
        Bands: ["NDVI"]
        Label: 2


        @param num_trees:

        @param max_variables:
        Specifies how many split variables will be used at a node.
        Default value is `null`, which corresponds to the number of predictors divided by 3.

        @param seed:
        Random seed for bootstrapping and choosing feature subsets.
        Set as None to generate seed based on system time. (default: None)
        """
        # TODO: Implement max_variables parameter
        features: List[List[float]] = self.prepare_for_json()
        labels: List[int] = [feature["properties"]["target"] for feature in target["features"]]

        # 1. Check if input is correct.
        if len(self._regions) != len(target["features"]):
            raise ProcessParameterInvalidException(
                parameter='target', process='fit_class_random_forest',
                reason="Predictor and target vector cubes should contain the same number of geometries.")
        if any(len(feature) == 0 for feature in features):
            raise ProcessParameterInvalidException(
                parameter='predictors', process='fit_class_random_forest',
                reason="One of the rows in the 'predictors' parameter is empty. This could be because there was no "
                       "data available for a geometry (on all bands).")
        if any(len(features[0]) != len(i) for i in features):
            raise ProcessParameterInvalidException(
                parameter='predictors', process='fit_class_random_forest',
                reason="One of the rows in the 'predictors' parameter contains an empty cell. This could be because "
                       "there was no data available for a geometry (on at least one band).")

        # 2. Create labeled data.
        labeled_data = [LabeledPoint(label, feature) for label, feature in zip(labels, features)]

        # 3. Train the model.
        num_classes = max(labels) + 1
        categorical_features_info = {}
        feature_subset_strategy = "auto"
        impurity = "gini"
        max_depth = 4
        max_bins = 32
        model = RandomForest.trainClassifier(
            gps.get_spark_context().parallelize(labeled_data),
            num_classes, categorical_features_info, num_trees,
            feature_subset_strategy, impurity, max_depth, max_bins, seed
            )
        return GeopySparkMLModel(model)
