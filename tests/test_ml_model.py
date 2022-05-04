import tempfile
from pathlib import Path
from random import uniform, seed
from typing import List
from unittest import TestCase
from unittest.mock import patch

import mock
import pytest
import shapely.geometry
from py4j.java_gateway import JavaObject
from shapely.geometry import GeometryCollection, Point
from openeo_driver.utils import EvalEnv
from pyspark.mllib.tree import RandomForestModel

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.ml.AggregateSpatialVectorCube import AggregateSpatialVectorCube
from openeogeotrellis.ml.GeopySparkCatBoostModel import GeopySparkCatBoostModel
from openeogeotrellis.ml.GeopySparkRandomForestModel import GeopySparkRandomForestModel

FEATURE_COLLECTION_1 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"target": 3},
            "geometry": {"type": "Polygon", "coordinates": [[[0.1, 0.1], [1.0, 0.1], [1.0, 1.0], [0.1, 1.0], [0.1, 0.1]]]}
        },
        {
            "type": "Feature",
            "properties": {"target": 5},
            "geometry": {"type": "Polygon", "coordinates": [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]]]}
        },
    ]
}


class DummyAggregateSpatialVectorCube(AggregateSpatialVectorCube):

    def prepare_for_json(self) -> List[List[float]]:
        return [[uniform(0.0, 1000.0), uniform(0.0, 1000.0)] for i in range(1000)]


class MockResponse:
    def __init__(self, data, status_code):
        self.data = data
        self.content = data
        self.status_code = status_code

    def json(self):
        return self.data


def mocked_requests_get_catboost(*args, **kwargs):
    item_url = 'https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json'
    asset_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/assets/catboost_model.tar.gz"
    if args[0] == item_url:
        metadata = GeopySparkCatBoostModel(None).get_model_metadata("./")
        metadata["assets"]["model"]["href"] = asset_url
        return MockResponse(metadata, 200)
    elif args[0] == asset_url:
        source_path = Path("tests/data/catboost_model.tar.gz")
        with open(source_path, 'rb') as f:
            model = f.read()
        return MockResponse(model, 200)
    return MockResponse(None, 404)


@mock.patch('openeogeotrellis.backend.requests.get', side_effect=mocked_requests_get_catboost)
def test_load_ml_model_for_catboost(mock_get, backend_implementation):
    request_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json"
    catboost_model = backend_implementation.load_ml_model(request_url)
    assert isinstance(catboost_model, JavaObject)


class TestFitClassRandomForestFlow(TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory("openeo-pydrvr-pytest-")
        self.patcher = patch('openeogeotrellis.backend.requests.get', side_effect=self.mocked_requests_get_flow)
        self.mock_get = self.patcher.start()
        self.addCleanup(self.patcher .stop)

    def mocked_requests_get_flow(self, *args, **kwargs):
        item_url = 'https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json'
        asset_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/assets/randomforest.model.tar.gz"
        file_name = asset_url.split('/')[-1]
        if args[0] == item_url:
            metadata = GeopySparkRandomForestModel(None).get_model_metadata("./")
            metadata["assets"]["model"]["href"] = asset_url
            return MockResponse(metadata, 200)
        elif args[0] == asset_url:
            source_path = Path(str(self.tmp_dir.name) + "/" + file_name)
            with open(source_path, 'rb') as f:
                model = f.read()
            return MockResponse(model, 200)
        return MockResponse(None, 404)

    @pytest.mark.usefixtures("backend_implementation", "imagecollection_with_two_bands_and_one_date")
    def test_fit_class_random_forest_flow(self):
        # 1. Generate features.
        int_cube = self.imagecollection_with_two_bands_and_one_date
        cube_xybt: GeopysparkDataCube = int_cube.apply_to_levels(
            lambda layer: int_cube._convert_celltype(layer, "float32"))
        geojson = {
            'type': 'GeometryCollection',
            'geometries': [feature['geometry'] for feature in FEATURE_COLLECTION_1['features']]
        }
        geometries = shapely.geometry.shape(geojson)
        mean_reducer = {
            "mean1": {
                "process_id": "mean",
                "arguments": {"data": {"from_argument": "data"}},
                "result": True
            }
        }
        cube_xyb = cube_xybt.reduce_dimension(mean_reducer, 't', EvalEnv())
        predictors: AggregateSpatialVectorCube = cube_xyb.aggregate_spatial(geometries, mean_reducer, "bands")
        # 2. Fit model.
        result: GeopySparkRandomForestModel = predictors.fit_class_random_forest(FEATURE_COLLECTION_1, num_trees=3, seed=42)
        assert (predictors.prepare_for_json() == [[1.0, 2.0], [1.0, 2.0]])
        assert (result.get_model().predict([2.0, 2.0]) == 5)
        # 3. Save model.
        result.save_ml_model(self.tmp_dir.name + "/job_metadata.json")
        print(self.tmp_dir.name)
        # 4. Load model.
        request_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json"
        self.backend_implementation.load_ml_model(request_url)

    def tearDown(self):
        self.tmp_dir.cleanup()


def test_fit_class_random_forest_results():
    # 1. Generate features and targets.
    seed(42)
    geometries = GeometryCollection([Point(i,i,i) for i in range(1000)])
    predictors = DummyAggregateSpatialVectorCube("", geometries)
    values: List[List[float]] = predictors.prepare_for_json()
    target = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "properties": {"target": i}} for i in range(1000)]
    }
    # 2. Fit model.
    num_trees = 3
    result: GeopySparkRandomForestModel = predictors.fit_class_random_forest(target, num_trees=num_trees, seed=42)
    # 3. Test model.
    model: RandomForestModel = result.get_model()
    assert(model.numTrees() == num_trees)
    assert(model.predict([0.0, 1.0]) == 110.0)
    assert(model.predict([122.5, 150.3]) == 107.0)
    assert(model.predict([565.5, 400.3]) == 7.0)
