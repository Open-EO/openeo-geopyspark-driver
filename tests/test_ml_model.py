import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from random import uniform, seed
from typing import List
from unittest import TestCase, skip
from unittest.mock import patch

import geopyspark
import mock
import pytest
import shapely.geometry
from openeo.metadata import CollectionMetadata, Dimension, TemporalDimension
from py4j.java_gateway import JavaObject
from shapely.geometry import GeometryCollection, Point

from openeo_driver.backend import BatchJobMetadata
from openeo_driver.save_result import MlModelResult
from openeo_driver.testing import TEST_USER_AUTH_HEADER, ApiTester, RegexMatcher, DictSubSet
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


def mock_catboost_job_results(*args, **kwargs):
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
@mock.patch('openeogeotrellis.backend.requests.get', side_effect=mock_catboost_job_results)
def test_load_ml_model_for_catboost(mock_get, backend_implementation):
    request_url = "https://openeo-test.vito.be/openeo/1.1.0/jobs/1234/results/items/ml_model_metadata.json"
    catboost_model = backend_implementation.load_ml_model(request_url)
    assert isinstance(catboost_model, JavaObject)


@skip("fit_class_catboost is not implemented.")
@mock.patch('openeo_driver.ProcessGraphDeserializer.evaluate')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_info')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_output_dir')
@mock.patch('openeogeotrellis.ml.GeopySparkCatBoostModel.GeopySparkCatBoostModel.write_assets')
def test_fit_class_catboost_batch_job_metadata(write_assets, get_job_output_dir, get_job_info, evaluate, tmp_path, client):
    # Note: Currently only catboost inference is supported so this metadata is not used.
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
        "processing:facility": "VITO - SPARK",
        "processing:software": RegexMatcher(r"openeo-geotrellis-[0-9a-z.]+"),
        'unique_process_ids': ['discard_result'],
        'ml_model_metadata': {
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
        },
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


@mock.patch('openeogeotrellis.layercatalog.GeoPySparkLayerCatalog.load_collection')
@mock.patch('openeo_driver.backend.CollectionCatalog.get_collection_metadata')
def test_fit_class_random_forest_synchronous(get_collection_metadata, load_collection, imagecollection_with_two_bands_and_one_date, backend_implementation, client, tmp_path):
    # 1. Create a request with the fit_class_random_forest process.
    cube_xybt: GeopysparkDataCube = imagecollection_with_two_bands_and_one_date.apply_to_levels(
        lambda layer: imagecollection_with_two_bands_and_one_date._convert_celltype(layer, "float32"))
    load_collection.return_value = cube_xybt
    get_collection_metadata.return_value = cube_xybt.metadata._orig_metadata
    request = {
        'process': {
            'process_graph': {
                'loadcollection1': {
                    'process_id': 'load_collection', 'arguments': {
                        'id': 'PROBAV_L3_S10_TOC_NDVI_333M',
                        'spatial_extent': {'west': 4.78, 'east': 4.91, 'south': 51.25, 'north': 51.31},
                        'temporal_extent': ['2017-11-01', '2017-11-01']
                    }
                }, 'reducedimension1': {
                    'process_id': 'reduce_dimension', 'arguments': {
                        'data': {'from_node': 'loadcollection1'}, 'dimension': 't', 'reducer': {
                            'process_graph': {
                                'mean1': {
                                    'process_id': 'mean', 'arguments': {'data': {'from_parameter': 'data'}},
                                    'result': True
                                }
                            }
                        }
                    }
                }, 'aggregatespatial1': {
                    'process_id': 'aggregate_spatial', 'arguments': {
                        'data': {'from_node': 'reducedimension1'},
                        'geometries': FEATURE_COLLECTION_1,
                        'reducer': {
                            'process_graph': {
                                'mean2': {
                                    'process_id': 'mean', 'arguments': {'data': {'from_parameter': 'data'}},
                                    'result': True
                                }
                            }
                        }, 'target_dimension': 'bands'
                    }
                }, 'fitclassrandomforest1': {
                    'process_id': 'fit_class_random_forest', 'arguments': {
                        'num_trees': 3,
                        'predictors': {'from_node': 'aggregatespatial1'},
                        'seed': 42,
                        'target': FEATURE_COLLECTION_1
                    },
                    'result': True
                }
            }
        }
    }

    # 2. Post a synchronous job to train a random forest model.
    api = ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)
    res = api.post('/result', json=request, headers = TEST_USER_AUTH_HEADER).assert_status_code(200)

    # 3. Load the response as a random forest model.
    dest_path = tmp_path / "randomforest.model.tar.gz"
    with open(dest_path, 'wb') as f:
        f.write(res.data)
    shutil.unpack_archive(dest_path, extract_dir = tmp_path, format = 'gztar')
    unpacked_model_path = str(dest_path).replace(".tar.gz", "")
    result_model = RandomForestModel.load(sc = geopyspark.get_spark_context(), path = "file:" + unpacked_model_path)

    # 4. Perform some inference locally to check if the model is correct.
    assert result_model.predict([0.0, 1.0]) == 5.0


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
def test_fit_class_random_forest_batch_job_metadata(get_job_output_dir, get_job_info, evaluate, tmp_path, client):
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

    size = (tmp_path / "randomforest.model.tar.gz").stat().st_size

    # 2. Check the job_metadata file that was written by batch_job.py
    metadata_result = read_json(tmp_path / JOB_METADATA_FILENAME)
    model_id = metadata_result['ml_model_metadata']['id']
    assert re.match(r'rf-[0-9a-f]{32}', model_id)
    metadata_result['ml_model_metadata']['id'] = 'rf-uuid'
    assert metadata_result == DictSubSet({
        'geometry': None, 'bbox': None, 'area': None, 'start_datetime': None, 'end_datetime': None, 'links': [],
        'assets': {
            'randomforest.model.tar.gz': {
                'href': str(tmp_path / "randomforest.model.tar.gz")
            }
        }, 'epsg': None, 'instruments': [], 'processing:facility': 'VITO - SPARK',
        "processing:software": RegexMatcher(r"openeo-geotrellis-[0-9a-z.]+"),
        'unique_process_ids': ['discard_result'],
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
    })

    # 3. Check the actual result returned by the /jobs/{j}/results endpoint.
    # It uses the job_metadata file as a basis to fill in the ml_model metadata fields.
    get_job_info.return_value = BatchJobMetadata(id=job_id, status='finished', created = datetime.now())
    api = ApiTester(api_version="1.1.0", client=client, data_root=TEST_DATA_ROOT)
    res = api.get('/jobs/{j}/results'.format(j = job_id), headers = TEST_USER_AUTH_HEADER).assert_status_code(200).json
    assert res == DictSubSet({
        'assets': {
            'randomforest.model.tar.gz': {
                'file:size': size,
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
    })

    item_res = api.get('/jobs/{j}/results/items/ml_model_metadata.json'.format(j = job_id), headers = TEST_USER_AUTH_HEADER).assert_status_code(200).json
    assert item_res['id'] == model_id
    item_res['id'] = 'rf-uuid'
    assert item_res == DictSubSet({
        'assets': {
            'model': {
                'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/assets/randomforest.model.tar.gz'.format(job_id = job_id),
                'roles': ['ml-model:checkpoint'], 'title': 'org.apache.spark.mllib.tree.model.RandomForestModel',
                'type': 'application/octet-stream'
            }
        }, 'bbox': [-179.999, -89.999, 179.999, 89.999], 'collection': job_id, 'geometry': {
            'coordinates': [
                [[-179.999, -89.999], [179.999, -89.999], [179.999, 89.999], [-179.999, 89.999], [-179.999, -89.999]]],
            'type': 'Polygon'
        }, 'id': 'rf-uuid', 'links': [],
        'properties': {
            'datetime': None, 'end_datetime': '9999-12-31T23:59:59Z', 'ml-model:architecture': 'random-forest',
            'ml-model:learning_approach': 'supervised', 'ml-model:prediction_type': 'classification',
            'ml-model:training-os': 'linux', 'ml-model:training-processor-type': 'cpu', 'ml-model:type': 'ml-model',
            'start_datetime': '1970-01-01T00:00:00Z'
        }, 'stac_extensions': ['https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'],
        'stac_version': '1.0.0', 'type': 'Feature'
    })
