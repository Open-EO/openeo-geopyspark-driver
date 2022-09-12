import re
import tempfile
from datetime import datetime
from pathlib import Path
from random import uniform, seed
from typing import List
from unittest import TestCase, skip
from unittest.mock import patch

import mock
import pytest
import shapely.geometry
from py4j.java_gateway import JavaObject
from shapely.geometry import GeometryCollection, Point

from openeo_driver.backend import BatchJobMetadata
from openeo_driver.save_result import MlModelResult
from openeo_driver.testing import TEST_USER_AUTH_HEADER, ApiTester
from openeo_driver.utils import EvalEnv, read_json
from pyspark.mllib.tree import RandomForestModel

from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.deploy.batch_job import run_job
from openeogeotrellis.geopysparkdatacube import GeopysparkDataCube
from openeogeotrellis.ml.AggregateSpatialVectorCube import AggregateSpatialVectorCube
from openeogeotrellis.ml.GeopySparkCatBoostModel import GeopySparkCatBoostModel
from openeogeotrellis.ml.GeopySparkRandomForestModel import GeopySparkRandomForestModel
from tests.data import TEST_DATA_ROOT

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
    asset_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/assets/catboost_model.cbm"
    if args[0] == item_url:
        metadata = GeopySparkCatBoostModel(None).get_model_metadata("./")
        metadata["assets"]["model"]["href"] = asset_url
        return MockResponse(metadata, 200)
    elif args[0] == asset_url:
        source_path = Path("tests/data/catboost_model.cbm")
        with open(source_path, 'rb') as f:
            model = f.read()
        return MockResponse(model, 200)
    return MockResponse(None, 404)


@skip("Causes permission error when creating folder under /data/projects/OpenEO/")
@mock.patch('openeogeotrellis.backend.requests.get', side_effect=mocked_requests_get_catboost)
def test_load_ml_model_for_catboost(mock_get, backend_implementation):
    request_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json"
    catboost_model = backend_implementation.load_ml_model(request_url)
    assert isinstance(catboost_model, JavaObject)

@mock.patch('openeo_driver.ProcessGraphDeserializer.evaluate')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_info')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_output_dir')
@mock.patch('openeogeotrellis.ml.GeopySparkCatBoostModel.GeopySparkCatBoostModel.write_assets')
def test_fit_class_catboost_job_metadata(write_assets, get_job_output_dir, get_job_info, evaluate, tmp_path, client):
    # Note: Catboost metadata is not yet used in production as currently only catboost inference is supported.
    # 1. Run a batch job, which will create a job_metadata.json file.
    evaluate.return_value = MlModelResult(GeopySparkCatBoostModel(None))
    job_id = "jobid"
    get_job_output_dir.return_value = tmp_path
    # TODO: Currently only inference is supported, so we mock training + saving here.
    write_assets.return_value = {"catboost_model.cbm": {"href": "catboost_model.cbm"}}

    run_job(
        job_specification={'process_graph': {'nop': {'process_id': 'discard_result', 'result': True}}},
        output_file=tmp_path /"out", metadata_file=tmp_path / JOB_METADATA_FILENAME, api_version="1.0.0", job_dir="./",
        dependencies={}, user_id="jenkins"
    )

    # 2. Check the job_metadata file that was written by batch_job.py
    metadata_result = read_json(tmp_path / JOB_METADATA_FILENAME)
    assert re.match(r'cb-[0-9a-f]{32}', metadata_result['ml_model_metadata']['id'])
    metadata_result['ml_model_metadata']['id'] = 'cb-uuid'
    assert metadata_result == {
        'geometry': None, 'bbox': None, 'area': None, 'start_datetime': None, 'end_datetime': None, 'links': [],
        'assets': {'catboost_model.cbm': {'href': 'catboost_model.cbm'}}, 'epsg': None, 'instruments': [],
        'processing:facility': 'VITO - SPARK', 'processing:software': 'openeo-geotrellis-0.6.4a1',
        'unique_process_ids': ['discard_result'], 'ml_model_metadata': {
            'stac_version': '1.0.0',
            'stac_extensions': ['https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'], 'type': 'Feature',
            'id': 'cb-uuid', 'collection': 'collection-id', 'bbox': [-179.999, -89.999, 179.999, 89.999], 'geometry': {
                'type': 'Polygon', 'coordinates': [
                    [[-179.999, -89.999], [179.999, -89.999], [179.999, 89.999], [-179.999, 89.999],
                     [-179.999, -89.999]]]
            }, 'properties': {
                'datetime': None, 'start_datetime': '1970-01-01T00:00:00Z', 'end_datetime': '9999-12-31T23:59:59Z',
                'ml-model:type': 'ml-model', 'ml-model:learning_approach': 'supervised',
                'ml-model:prediction_type': 'classification', 'ml-model:architecture': 'catboost',
                'ml-model:training-processor-type': 'cpu', 'ml-model:training-os': 'linux'
            }, 'links': [], 'assets': {
                'model': {
                    'href': str(tmp_path / 'catboost_model.cbm'),
                    'type': 'application/octet-stream', 'title': 'ai.catboost.spark.CatBoostClassificationModel',
                    'roles': ['ml-model:checkpoint']
                }
            }
        }
    }

    # 3. Check the actual result returned by the /jobs/{j}/results endpoint.
    # It uses the job_metadata file as a basis to fill in the ml_model metadata fields.
    get_job_info.return_value = BatchJobMetadata(id=job_id, status='finished', created = datetime.now())
    api = ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)
    res = api.get('/jobs/{j}/results'.format(j = job_id), headers = TEST_USER_AUTH_HEADER).assert_status_code(200).json
    assert res == {
        'assets': {
            'catboost_model.cbm': {
                'file:nodata': [None],
                'href': 'http://oeo.net/openeo/1.1.0/jobs/jobid/results/assets/catboost_model.cbm', 'roles': ['data'],
                'title': 'catboost_model.cbm', 'type': 'application/octet-stream'
            }
        }, 'description': 'Results for batch job {job_id}'.format(job_id=job_id),
        'extent': {'spatial': {'bbox': [None]}, 'temporal': {'interval': [[None, None]]}}, 'id': job_id,
        'license': 'proprietary',
        'links': [{'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results'.format(job_id=job_id), 'rel': 'self', 'type': 'application/json'},
                  {
                      'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results'.format(job_id=job_id), 'rel': 'canonical',
                      'type': 'application/json'
                  }, {
                      'href': 'http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf',
                      'rel': 'card4l-document', 'type': 'application/pdf'
                  }, {
                      'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/items/ml_model_metadata.json'.format(job_id=job_id),
                      'rel': 'item', 'type': 'application/json'
                  }],
        'stac_extensions': ['eo', 'file', 'https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'],
        'stac_version': '1.0.0', 'summaries': {
            'ml-model:architecture': ['catboost'], 'ml-model:learning_approach': ['supervised'],
            'ml-model:prediction_type': ['classification']
        }, 'type': 'Collection'
    }

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

    @skip("Causes permission error when creating folder under /data/projects/OpenEO/")
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
        result.write_assets(self.tmp_dir.name + "/job_metadata.json")
        print(self.tmp_dir.name)
        # 4. Load model.
        request_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json"
        self.backend_implementation.load_ml_model(request_url)

    def tearDown(self):
        self.tmp_dir.cleanup()

def train_simple_random_forest_model(num_trees = 3, seedValue = 42, nrGeometries = 1000) -> GeopySparkRandomForestModel:
    # 1. Generate features and targets.
    seed(seedValue)
    geometries = GeometryCollection([Point(i,i,i) for i in range(nrGeometries)])
    predictors = DummyAggregateSpatialVectorCube("", geometries)
    target = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "properties": {"target": i}} for i in range(nrGeometries)]
    }
    # 2. Fit model.
    return predictors.fit_class_random_forest(target, num_trees=num_trees, seed=seedValue)

def test_fit_class_random_forest_model():
    """
    Sanity check to ensure that the model returned by fit_class_random_forest is valid
    and provides the correct predictions.
    """
    num_trees = 3
    result: GeopySparkRandomForestModel = train_simple_random_forest_model(num_trees = num_trees, seedValue = 42)
    # Test model.
    model: RandomForestModel = result.get_model()
    assert(model.numTrees() == num_trees)
    assert(model.predict([0.0, 1.0]) == 980.0)
    assert(model.predict([122.5, 150.3]) == 752.0)
    assert(model.predict([565.5, 400.3]) == 182.0)

@mock.patch('openeo_driver.ProcessGraphDeserializer.evaluate')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_info')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_output_dir')
def test_fit_class_random_forest_job_metadata(get_job_output_dir, get_job_info, evaluate, tmp_path, client):
    # 1. Run a batch job, which will create a job_metadata.json file.
    random_forest_model: GeopySparkRandomForestModel = train_simple_random_forest_model(3, 42)
    evaluate.return_value = MlModelResult(random_forest_model)
    job_id = "jobid"
    get_job_output_dir.return_value = tmp_path

    run_job(
        job_specification={'process_graph': {'nop': {'process_id': 'discard_result', 'result': True}}},
        output_file=tmp_path /"out", metadata_file=tmp_path / JOB_METADATA_FILENAME, api_version="1.0.0", job_dir="./",
        dependencies={}, user_id="jenkins"
    )

    # 2. Check the job_metadata file that was written by batch_job.py
    metadata_result = read_json(tmp_path / JOB_METADATA_FILENAME)
    assert re.match(r'rf-[0-9a-f]{32}', metadata_result['ml_model_metadata']['id'])
    metadata_result['ml_model_metadata']['id'] = 'rf-uuid'
    assert metadata_result == {
        'geometry': None, 'bbox': None, 'area': None, 'start_datetime': None, 'end_datetime': None, 'links': [],
        'assets': {
            'randomforest.model.tar.gz': {
                'href': str(tmp_path / "randomforest.model.tar.gz")
            }
        }, 'epsg': None, 'instruments': [], 'processing:facility': 'VITO - SPARK',
        'processing:software': 'openeo-geotrellis-0.6.4a1', 'unique_process_ids': ['discard_result'],
        'ml_model_metadata': {
            'stac_version': '1.0.0',
            'stac_extensions': ['https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'], 'type': 'Feature',
            'id': 'rf-uuid', 'collection': 'collection-id',
            'bbox': [-179.999, -89.999, 179.999, 89.999],
            'geometry': {
                'type': 'Polygon', 'coordinates': [
                    [[-179.999, -89.999], [179.999, -89.999], [179.999, 89.999], [-179.999, 89.999],
                     [-179.999, -89.999]]]
            }, 'properties': {
                'datetime': None, 'start_datetime': '1970-01-01T00:00:00Z', 'end_datetime': '9999-12-31T23:59:59Z',
                'ml-model:type': 'ml-model', 'ml-model:learning_approach': 'supervised',
                'ml-model:prediction_type': 'classification', 'ml-model:architecture': 'random-forest',
                'ml-model:training-processor-type': 'cpu', 'ml-model:training-os': 'linux'
            }, 'links': [], 'assets': {
                'model': {
                    'title': 'org.apache.spark.mllib.tree.model.RandomForestModel',
                    'href': str(tmp_path / 'randomforest.model.tar.gz'),
                    'type': 'application/octet-stream',
                    'roles': ['ml-model:checkpoint']
                }
            }
        }
    }

    # 3. Check the actual result returned by the /jobs/{j}/results endpoint.
    # It uses the job_metadata file as a basis to fill in the ml_model metadata fields.
    get_job_info.return_value = BatchJobMetadata(id=job_id, status='finished', created = datetime.now())
    api = ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)
    res = api.get('/jobs/{j}/results'.format(j = job_id), headers = TEST_USER_AUTH_HEADER).assert_status_code(200).json
    assert res == {
        'assets': {
            'randomforest.model.tar.gz': {
                'file:nodata': [None],
                'href': 'http://oeo.net/openeo/1.1.0/jobs/jobid/results/assets/randomforest.model.tar.gz',
                'roles': ['data'], 'title': 'randomforest.model.tar.gz', 'type': 'application/octet-stream'
            }
        }, 'description': 'Results for batch job {job_id}'.format(job_id=job_id),
        'extent': {
            'spatial': {'bbox': [None]},
                   'temporal': {'interval': [[None, None]]}}, 'id': job_id,
        'license': 'proprietary',
        'links': [{'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results'.format(job_id=job_id), 'rel': 'self', 'type': 'application/json'},
                  {
                      'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results'.format(job_id=job_id), 'rel': 'canonical',
                      'type': 'application/json'
                  }, {
                      'href': 'http://ceos.org/ard/files/PFS/SR/v5.0/CARD4L_Product_Family_Specification_Surface_Reflectance-v5.0.pdf',
                      'rel': 'card4l-document', 'type': 'application/pdf'
                  }, {
                      'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/items/ml_model_metadata.json'.format(job_id=job_id),
                      'rel': 'item', 'type': 'application/json'
                  }],
        'stac_extensions': ['eo', 'file', 'https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'],
        'stac_version': '1.0.0',
        'summaries': {
            'ml-model:architecture': ['random-forest'],
            'ml-model:learning_approach': ['supervised'],
            'ml-model:prediction_type': ['classification']
        }, 'type': 'Collection'
    }
