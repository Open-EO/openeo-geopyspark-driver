from random import uniform, seed
from typing import List
import shapely.geometry
from shapely.geometry import GeometryCollection, Point
from openeo_driver.utils import EvalEnv
from pyspark.mllib.tree import RandomForestModel

from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.ml_model import AggregateSpatialVectorCube, GeopySparkMLModel

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


def test_fit_class_random_forest_flow(imagecollection_with_two_bands_and_one_date):
    # 1. Generate features.
    int_cube = imagecollection_with_two_bands_and_one_date
    cube_xybt: GeopysparkDataCube = int_cube.apply_to_levels(lambda layer: int_cube._convert_celltype(layer, "float32"))
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
    result: GeopySparkMLModel = predictors.fit_class_random_forest(FEATURE_COLLECTION_1, num_trees=3, seed=42)
    assert(predictors.prepare_for_json() == [[1.0, 2.0], [1.0, 2.0]])
    assert(result.get_model().predict([2.0, 2.0]) == 5)

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
    result: GeopySparkMLModel = predictors.fit_class_random_forest(target, num_trees=num_trees, seed=42)
    # 3. Test model.
    model: RandomForestModel = result.get_model()
    assert(model.numTrees() == num_trees)
    assert(model.predict([0.0, 1.0]) == 110.0)
    assert(model.predict([122.5, 150.3]) == 107.0)
    assert(model.predict([565.5, 400.3]) == 7.0)
