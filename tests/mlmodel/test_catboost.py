import re
from pathlib import Path
from typing import List, Iterator
from unittest import skip

import mock
import pytest
from openeo_driver.constants import JOB_STATUS
from openeo_driver.save_result import MlModelResult
from openeo_driver.testing import (
    TEST_USER_AUTH_HEADER,
    DictSubSet,
    ListSubSet,
    TEST_USER,
)
from openeo_driver.utils import read_json
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from shapely.geometry import GeometryCollection, Point

import openeogeotrellis.ml.catboost_spark as catboost_spark
from openeogeotrellis.backend import JOB_METADATA_FILENAME
from openeogeotrellis.deploy.batch_job import run_job
from openeogeotrellis.job_registry import DoubleJobRegistry, ZkJobRegistry
from openeogeotrellis.ml.aggregatespatialvectorcube import AggregateSpatialVectorCube
from openeogeotrellis.ml.geopysparkcatboostmodel import GeopySparkCatBoostModel
from openeogeotrellis.testing import KazooClientMock

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


@pytest.fixture
def zk_client() -> Iterator[KazooClientMock]:
    zk_client = KazooClientMock()
    with mock.patch("openeogeotrellis.job_registry.KazooClient", return_value=zk_client):
        yield zk_client


class DummyAggregateSpatialVectorCube(AggregateSpatialVectorCube):

    def prepare_for_json(self) -> List[List[float]]:
        # Return 2 random band values for every polygon in the vector cube.
        return [[i%20,i*2+1%30] for i in range(1000)]


def train_simple_catboost_model() -> GeopySparkCatBoostModel:
    # 1. Generate features and targets.
    geometries = GeometryCollection([Point(i,i,i) for i in range(1000)])
    predictors = DummyAggregateSpatialVectorCube("", geometries)
    target = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "properties": {"target": i}} for i in range(1000)]
    }
    # 2. Fit model.
    return predictors.fit_class_catboost(target, iterations=1)


@skip("not yet supported in spark 4")
def test_fit_class_catboost_model():
    """
    Sanity check to ensure that the model returned by fit_class_catboost is valid
    and provides the correct predictions.
    """
    result: GeopySparkCatBoostModel = train_simple_catboost_model()
    model: catboost_spark.CatBoostClassificationModel = result.get_model()
    assert model.numClasses == 1000
    assert model.predict(Vectors.dense([2.0,5.0])) == 161.0
    assert model.predict(Vectors.dense([122.5, 150.3])) == 10.0
    assert model.predict(Vectors.dense([565.5, 400.3])) == 10.0


@skip("not yet supported in spark 4")
@mock.patch('openeo_driver.ProcessGraphDeserializer.evaluate')
@mock.patch('openeogeotrellis.backend.GpsBatchJobs.get_job_output_dir')
def test_fit_class_catboost_batch_job_metadata(
    get_job_output_dir, evaluate, tmp_path: Path, api110, zk_client, job_registry
):
    """
    Test the metadata generation for a CatBoost model trained using a batch job.

    This test performs the following steps:
    1. Runs a batch job while mocking the evaluate step. So it only generates a job_metadata.json file.
    2. Verifies "job_metadata.json"
    3. Checks /jobs/{job_id}/results endpoint, ensuring it correctly includes model metadata.
    4. Checks /jobs/{job_id}/results/items.
    """
    # # 1. Run a batch job, which will create a job_metadata.json file.
    evaluate.return_value = MlModelResult(train_simple_catboost_model())
    job_id = "jobid"
    get_job_output_dir.return_value = tmp_path
    run_job(
        job_specification={'process_graph': {'nop': {'process_id': 'discard_result', 'result': True}}},
        output_file=tmp_path /"out", metadata_file=tmp_path / JOB_METADATA_FILENAME, api_version="1.0.0", job_dir="./",
        dependencies={}, user_id="jenkins"
    )

    # 2. Check the job_metadata file that was written by batch_job.py
    metadata_result = read_json(tmp_path / JOB_METADATA_FILENAME)
    model_id = metadata_result["ml_model_metadata"]["id"]
    assert re.match(r'cb-[0-9a-f]{32}', model_id)

    assert metadata_result == DictSubSet({
        'assets': {'catboost_model.cbm.tar.gz': {'href': str(tmp_path / 'catboost_model.cbm.tar.gz')}},
        'ml_model_metadata': DictSubSet({
            'stac_extensions': ['https://stac-extensions.github.io/ml-model/v1.0.0/schema.json'],
            'type': 'Feature',
            'collection': 'collection-id',
            'bbox': [-179.999, -89.999, 179.999, 89.999],
            'geometry': {
                'type': 'Polygon',
                'coordinates': [
                    [[-179.999, -89.999], [179.999, -89.999], [179.999, 89.999], [-179.999, 89.999],
                     [-179.999, -89.999]]
                ]
            },
            'properties': {
                'datetime': None, 'start_datetime': '1970-01-01T00:00:00Z', 'end_datetime': '9999-12-31T23:59:59Z',
                'ml-model:type': 'ml-model',
                'ml-model:learning_approach': 'supervised',
                'ml-model:prediction_type': 'classification',
                'ml-model:architecture': 'catboost',
                'ml-model:training-processor-type': 'cpu',
                'ml-model:training-os': 'linux'
            },
            'links': [],
            'assets': {
                'model': {
                    'href': str(tmp_path / 'catboost_model.cbm.tar.gz'),
                    # 'href': '/tmp/pytest-of-jeroen/pytest-35/test_fit_class_catboost_batch_0/catboost_model.cbm',
                    'type': 'application/octet-stream',
                    'title': 'ai.catboost.spark.CatBoostClassificationModel',
                    'roles': ['ml-model:checkpoint']
                }
            }
        }),
    })

    # 3. Check the actual result returned by the /jobs/{j}/results endpoint.
    # It uses the job_metadata file as a basis to fill in the ml_model metadata fields.

    res = api110.post(
        "/jobs", json=api110.get_process_graph_dict({}), headers=TEST_USER_AUTH_HEADER
    ).assert_status_code(201)
    job_id = res.headers["OpenEO-Identifier"]

    dbl_job_registry = DoubleJobRegistry(
        # TODO #236/#498/#632 phase out ZkJobRegistry
        zk_job_registry_factory=(lambda: ZkJobRegistry(zk_client=zk_client)),
        elastic_job_registry=job_registry,
    )
    with dbl_job_registry:
        dbl_job_registry.set_status(job_id, user_id=TEST_USER, status=JOB_STATUS.FINISHED)

    res = api110.get('/jobs/{j}/results'.format(j=job_id), headers=TEST_USER_AUTH_HEADER).assert_status_code(200).json

    size = (tmp_path / "catboost_model.cbm.tar.gz").stat().st_size
    assert res == DictSubSet({
        'assets': {
            'catboost_model.cbm.tar.gz': {
                'title': 'catboost_model.cbm.tar.gz',
                'href': f'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/assets/catboost_model.cbm.tar.gz',
                'roles': ['data'], 'type': 'application/octet-stream',
                "file:size": size,
            }
        },
        'links': ListSubSet([{
            'href': 'http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/items/ml_model_metadata.json'.format(job_id=job_id),
            'rel': 'item', 'type': 'application/json'
        }]),
        'stac_extensions': ListSubSet([
            "https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"
        ]),
        'summaries': {
            'ml-model:architecture': ['catboost'],
            'ml-model:learning_approach': ['supervised'],
            'ml-model:prediction_type': ['classification']
        },
    })

    # 4. Check the item metadata returned by the /jobs/{j}/results/items endpoint.
    item_res = (
        api110.get("/jobs/{j}/results/items/ml_model_metadata.json".format(j=job_id), headers=TEST_USER_AUTH_HEADER)
        .assert_status_code(200)
        .json
    )
    assert item_res["id"] == model_id
    assert item_res == DictSubSet({
        "assets": {
            "model": {
                "href": f"http://oeo.net/openeo/1.1.0/jobs/{job_id}/results/assets/catboost_model.cbm.tar.gz",
                "roles": ["ml-model:checkpoint"],
                "title": "ai.catboost.spark.CatBoostClassificationModel",
                "type": "application/octet-stream",
            }
        },
        "bbox": [-179.999, -89.999, 179.999, 89.999],
        "collection": job_id,
        "geometry": {
            "coordinates": [
                [
                    [-179.999, -89.999],
                    [179.999, -89.999],
                    [179.999, 89.999],
                    [-179.999, 89.999],
                    [-179.999, -89.999],
                ]
            ],
            "type": "Polygon",
        },
        "properties": {
            "datetime": None,
            "end_datetime": "9999-12-31T23:59:59Z",
            "ml-model:architecture": "catboost",
            "ml-model:learning_approach": "supervised",
            "ml-model:prediction_type": "classification",
            "ml-model:training-os": "linux",
            "ml-model:training-processor-type": "cpu",
            "ml-model:type": "ml-model",
            "start_datetime": "1970-01-01T00:00:00Z",
        },
        "stac_extensions": ["https://stac-extensions.github.io/ml-model/v1.0.0/schema.json"],
        "type": "Feature",
    })


def test_load_ml_model(backend_implementation, urllib_and_request_mock, test_data):
    metadata = test_data.load_text("mlmodel/catboost/stac_catboost.json")
    model_data = test_data.load_bytes("mlmodel/catboost/catboost_model.cbm.tar.gz")
    prefix = "https://openeo.vito.be/openeo/1.2/jobs/j-25101315033849049a4a67ea6d24b7ca/results/items/ZGZhNjc4Y/"
    metadata_url = prefix + "catboost.json"
    urllib_and_request_mock.get(metadata_url, metadata)
    urllib_and_request_mock.get(prefix + "catboost_model.cbm.tar.gz", model_data)
    ml_model:GeopySparkCatBoostModel = backend_implementation.load_ml_model(metadata_url)
    assert isinstance(ml_model, GeopySparkCatBoostModel)
    assert isinstance(ml_model.get_model(), catboost_spark.CatBoostClassificationModel)
