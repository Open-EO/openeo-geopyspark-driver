import os
import subprocess
import sys
import textwrap
import zipfile
from pathlib import Path

import pytest
import rasterio

from openeo_driver.backend import LoadParameters
from openeo_driver.datastructs import SarBackscatterArgs
from openeo_driver.utils import EvalEnv
from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeo, _import_orfeo_toolbox
from openeogeotrellis.layercatalog import GeoPySparkLayerCatalog


@pytest.mark.skipif(not os.environ.get("CREODIAS"), reason="Requires CREODIAS environment.")
@pytest.mark.parametrize(["spatial_extent", "temporal_extent", "expected_shape"], [
    (
            dict(west=3.1, south=51.27, east=3.3, north=51.37),  # Zeebrugge
            ("2020-06-06T00:00:00", "2020-06-06T23:59:59"),
            (2, 1117, 1392),
    ),
    (
            dict(west=5.5, south=50.13, east=5.65, north=50.23),  # La Roche-en-Ardenne
            ("2020-07-29T00:00:00", "2020-07-29T23:59:59"),
            (2, 1150, 1033),
    ),
])
def test_creodias_s1_backscatter(tmp_path, spatial_extent, temporal_extent, expected_shape):
    catalog = GeoPySparkLayerCatalog(all_metadata=[{
        "id": "Creodias-S1-Backscatter",
        "_vito": {"data_source": {"type": 'creodias-s1-backscatter'}},
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x"},
            "y": {"type": "spatial", "axis": "y"},
            "t": {"type": "temporal"},
            "bands": {"type": "bands", "values": ["VH", "VV"]}
        }
    }])

    load_params = LoadParameters(
        temporal_extent=temporal_extent, spatial_extent=spatial_extent,
        sar_backscatter=SarBackscatterArgs(
            backscatter_coefficient="sigma0",
            orthorectify=True,
            options={"dem_zoom_level": 6}
        )
    )
    datacube = catalog.load_collection("Creodias-S1-Backscatter", load_params=load_params, env=EvalEnv())

    filename = tmp_path / "s1backscatter.tiff"
    datacube.save_result(filename, format="GTiff", format_options={'stitch': True})
    with rasterio.open(filename) as ds:
        assert ds.read().shape == expected_shape


@pytest.mark.parametrize(["bbox", "bbox_epsg"], [
    ((3.1, 51.2, 3.5, 51.3), 4326),
    ((506986, 5672070, 534857, 5683305), 32631),
])
def test_creodias_dem_subset_geotiff(bbox, bbox_epsg):
    dirs = set()
    symlinks = {}
    with S1BackscatterOrfeo._creodias_dem_subset_geotiff(
            bbox=bbox, bbox_epsg=bbox_epsg, zoom=11,
            dem_path_tpl="/path/to/geotiff/{z}/{x}/{y}.tif"
    ) as temp_dir:
        temp_dir = Path(temp_dir)
        for path in temp_dir.glob("**/*"):
            relative = path.relative_to(temp_dir)
            if path.is_dir():
                dirs.add(str(relative))
            elif path.is_symlink():
                symlinks[str(relative)] = os.readlink(path)
            else:
                raise ValueError(path)
    assert dirs == {"11", "11/1041", "11/1042", "11/1043"}
    assert symlinks == {
        "11/1041/682.tif": "/path/to/geotiff/11/1041/682.tif",
        "11/1041/683.tif": "/path/to/geotiff/11/1041/683.tif",
        "11/1042/682.tif": "/path/to/geotiff/11/1042/682.tif",
        "11/1042/683.tif": "/path/to/geotiff/11/1042/683.tif",
        "11/1043/682.tif": "/path/to/geotiff/11/1043/682.tif",
        "11/1043/683.tif": "/path/to/geotiff/11/1043/683.tif",
    }
    assert not temp_dir.exists()


@pytest.mark.parametrize(["bbox", "bbox_epsg", "expected"], [
    ((3.1, 51.2, 3.5, 51.3), 4326, {"N51E003.hgt"}),
    ((506986, 5672070, 534857, 5683305), 32631, {"N51E003.hgt"}),
    ((3.8, 51.9, 4.2, 52.1), 4326, {"N51E003.hgt", "N51E004.hgt", "N52E003.hgt", "N52E004.hgt", }),
    ((-3.1, -51.2, -3.5, -51.3), 4326, {"S51W003.hgt"}),
])
def test_creodias_dem_subset_srtm_hgt_unzip(bbox, bbox_epsg, expected, tmp_path):
    # Set up fake zips.
    for name in ["N51E003", "N51E004", "N52E003", "N52E004", "S51W003"]:
        with zipfile.ZipFile(tmp_path / f"{name}.SRTMGL1.hgt.zip", mode="w") as z:
            with z.open(f"{name}.hgt", mode="w") as f:
                f.write(b"hgt data here")

    # Check _creodias_dem_subset_srtm_hgt_unzip
    with S1BackscatterOrfeo._creodias_dem_subset_srtm_hgt_unzip(
            bbox=bbox, bbox_epsg=bbox_epsg, srtm_root=str(tmp_path)
    ) as temp_dem_dir:
        temp_dem_dir = Path(temp_dem_dir)
        assert set(os.listdir(temp_dem_dir)) == expected


def test_import_orfeo_toolbox(tmp_path, caplog):
    try:
        import otbApplication
        pytest.skip("`import otbApplication` works directly, so we can't (and don't need to) test the fallback mechanism.")
    except ImportError:
        pass

    # Set up fake otbApplication module
    otb_home = tmp_path / "otb_home"
    otb_module_path = otb_home / "lib/otb/python"
    otb_module_path.mkdir(parents=True)
    otb_module_src = textwrap.dedent("""\
        import os
        foo = os.environ["OTB_APPLICATION_PATH"]
    """)
    with (otb_module_path / "otbApplication.py").open("w") as f:
        f.write(otb_module_src)

    # Try importing (in a subprocess with isolated state regarding imported modules)
    test_script = textwrap.dedent("""
        from openeogeotrellis.collections.s1backscatter_orfeo import _import_orfeo_toolbox
        otb = _import_orfeo_toolbox()
        print(otb.foo)
    """)
    env = {**os.environ, **{"OTB_HOME": str(otb_home)}}
    p = subprocess.run(
        [sys.executable, "-c", test_script],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
    )
    expected_stdout = f"{otb_home}/lib/otb/applications\n"
    assert (p.returncode, p.stdout, p.stderr) == (0, expected_stdout.encode("utf8"), b"")
