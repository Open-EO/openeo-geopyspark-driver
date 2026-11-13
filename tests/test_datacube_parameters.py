import pytest
from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv

from openeogeotrellis import datacube_parameters
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.testing import gps_config_overrides
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
    ["config_default", "feature_flags", "expected"],
    [
        (None, {}, False),
        (True, {}, True),
        (False, {"retain_nodata_tiles": True}, True),
        (True, {"retain_nodata_tiles": False}, False),
    ],
)
def test_data_cube_params_retain_nodata_tiles(config_default, feature_flags, expected):
    load_params = LoadParameters(featureflags=feature_flags)
    env = EvalEnv({EVAL_ENV_KEY.REQUIRE_BOUNDS: True})

    overrides = {} if config_default is None else {"default_retain_nodata_tiles": config_default}
    with gps_config_overrides(**overrides):
        cube_params, _ = datacube_parameters.create(load_params, env, get_jvm())

    assert cube_params.retainNoDataTiles() == expected
