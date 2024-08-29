import logging
from typing import List, Optional, Union

import geopyspark as gps
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest

from openeo_driver.errors import ProcessParameterInvalidException
from openeo_driver.save_result import AggregatePolygonSpatialResult
from openeogeotrellis.ml.geopysparkrandomforestmodel import GeopySparkRandomForestModel
from openeogeotrellis.ml.geopysparkcatboostmodel import GeopySparkCatBoostModel
from openeogeotrellis.ml.catboost_spark import Pool, CatBoostClassifier

from pyspark.sql import Row,SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import *
from pyspark import SparkContext

logger = logging.getLogger(__name__)


class AggregateSpatialVectorCube(AggregatePolygonSpatialResult):
    # TODO: This is a temporary class until vector cubes are fully implemented.
    # TODO: move to openeogeotrellis.vectorcube module

    @classmethod
    def _check_features_and_labels(cls, features: List[List[float]], labels: List[int]):
        if len(labels) != len(features):
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

    def fit_class_random_forest(
            self,
            target: dict,
            num_trees: int = 100,
            max_variables: Optional[Union[int, str]] = None,
            seed: Optional[int] = None,
    ) -> 'GeopySparkRandomForestModel':
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
        logger.info(f"fit_class_random_forest features: {features}")
        logger.info(f"fit_class_random_forest labels: {labels}")

        # 1. Check if input is correct.
        self._check_features_and_labels(features, labels)

        # 2. Create labeled data.
        labeled_data = [LabeledPoint(label, feature) for label, feature in zip(labels, features)]
        logger.info(f"fit random forest labeled data: {labeled_data} seed: {seed} num_trees: {num_trees} max_variables: {max_variables}")

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
        return GeopySparkRandomForestModel(model)


    def fit_class_catboost(
            self, 
            target: dict,
            iterations: int = 5,
            depth=5,
            border_count=254,
            seed=0,
    ) -> 'GeopySparkCatBoostModel':
        features: List[List[float]] = self.prepare_for_json()
        labels: List[int] = [feature["properties"]["target"] for feature in target["features"]]

        # 1. Check if input is correct.
        self._check_features_and_labels(features, labels)

        # 2. Create labeled data.
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        srcDataSchema = [
            StructField("features", VectorUDT()),
            StructField("label", StringType())
        ]
        trainData = [Row(Vectors.dense(feature), str(label)) for feature, label in zip(features, labels)]
        trainDf = spark.createDataFrame(sc.parallelize(trainData), StructType(srcDataSchema))
        trainPool = Pool(trainDf)

        # 3. Train the model.
        num_classes = max(labels) + 1
        classifier = CatBoostClassifier(
            classesCount=num_classes,
            iterations=iterations,
            depth=depth,
            earlyStoppingRounds=2,
            borderCount=border_count,
            randomSeed=seed,
            lossFunction="MultiClass",
        )

        # train a model
        model = classifier.fit(trainPool)
        return GeopySparkCatBoostModel(model)
