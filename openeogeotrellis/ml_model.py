import pathlib
from typing import Dict, List, Optional

from openeo_driver.datacube import DriverMlModel
from openeo_driver.errors import ProcessParameterInvalidException
from openeo_driver.save_result import AggregatePolygonSpatialResult
import geopyspark as gps
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import JavaSaveable


class GeopySparkMLModel(DriverMlModel):

    def __init__(self, model: JavaSaveable):
        self._model = model

    def write_assets(self, directory: str) -> Dict:
        """
        Save generated assets into a directory, return asset metadata.

        :return: STAC assets dictionary: https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#assets
        """
        directory = pathlib.Path(directory).parent
        filename = "file:" + str(pathlib.Path(directory) / "randomforest.model")
        self._model.save(gps.get_spark_context(), filename)
        return {filename:{"href":filename}}

    def get_model(self):
        return self._model

    def save_ml_model(self, directory: str) -> Dict:
        return self.write_assets(directory)


class AggregateSpatialVectorCube(AggregatePolygonSpatialResult):
    """
    TODO: This is a temporary class until vector cubes are fully implemented.
    """

    def fit_class_random_forest(
            self, target: dict,
            training: float=0.8, num_trees: int = 100, mtry: Optional[int] = None, seed: Optional[int] = None
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

        @param training:
        The amount of training data to be used in the classification.
        The sampling will be chosen randomly through the data object.
        The remaining data will be used as test data for the validation.
        https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.randomSplit.html

        @param num_trees:

        @param mtry:
        Specifies how many split variables will be used at a node.
        Default value is `null`, which corresponds to the number of predictors divided by 3.

        @param seed:
        Random seed for bootstrapping and choosing feature subsets.
        Set as None to generate seed based on system time. (default: None)
        """
        # TODO: 1. Implement training parameter
        # = The amount of training data to be used in the classification.
        # The sampling will be chosen randomly through the data object.
        # The remaining data will be used as test data for the validation.
        # TODO: 2. Implement mtry parameter
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
