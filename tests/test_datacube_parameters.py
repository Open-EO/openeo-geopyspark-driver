from unittest.mock import MagicMock

import pytest
from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv

from openeogeotrellis import datacube_parameters
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.utils import get_jvm


def test_data_cube_params():
    load_params = LoadParameters(
        bands=["TOC-B03_10M"],
        resample_method="average",
        target_crs="EPSG:4326",
        global_extent={"east": 2.0, "west": 1.0, "south": 2.0, "north": 3.0, "crs": "EPSG:4326"},
        featureflags={"tilesize": 128},
    )
    env = EvalEnv({EVAL_ENV_KEY.REQUIRE_BOUNDS: True})

    cube_params, level = datacube_parameters.create(load_params, env, get_jvm())
    assert str(cube_params) == ("DataCubeParameters(128, {}, ZoomedLayoutScheme, ByDay, None, None, "
                                "Average, 0.0, 0.0, false, true, true)")
    assert "Average" == str(cube_params.resampleMethod())


@pytest.mark.parametrize(
    ["featureflags", "expected"],
    [
        ({}, False),
        ({"retain_nodata_tiles": False}, False),
        ({"retain_nodata_tiles": True}, True),
    ],
)
def test_retain_nodata_tiles_feature_flag(featureflags, expected):
    load_params = LoadParameters(featureflags=featureflags)

    cube_params, _ = datacube_parameters.create(load_params, EvalEnv({}), MagicMock())

    cube_params.setRetainNoDataTiles.assert_called_once_with(expected)
