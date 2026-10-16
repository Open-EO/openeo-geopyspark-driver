import datetime as dt
import shutil
from pathlib import Path
from unittest import mock

import pytest
from openeo_driver.backend import LoadParameters
from openeo_driver.utils import EvalEnv

from openeogeotrellis.collections.geotiff_files import (
    derive_date,
    list_disk_geotiffs,
    list_s3_geotiffs,
    load_geotiffs,
)
from openeogeotrellis.constants import EVAL_ENV_KEY
from openeogeotrellis.load_stac import NoDataAvailableException
from openeogeotrellis.utils import get_jvm

JOB_DIR = Path(__file__).parent.parent / "data" / "binary" / "jobs" / "j-ec5d3e778ba5423d8d88a50b08cb9f63"
DATE_REGEX = r".+_(\d{4})-(\d{2})-(\d{2})Z\.tif"


@pytest.fixture
def geotiff_dir(tmp_path) -> Path:
    for geotiff in JOB_DIR.glob("*.tif"):
        shutil.copy(geotiff, tmp_path)
    (tmp_path / "subdir.tif").mkdir()
    (tmp_path / "metadata.json").write_text("{}")
    return tmp_path


@pytest.fixture
def sentinel_hub_geotiff_dir(tmp_path) -> Path:
    """GeoTIFFs named like tiles assembled from the Sentinel Hub batch process cache."""
    for geotiff in JOB_DIR.glob("*.tif"):
        date = geotiff.stem.split("_")[1].rstrip("Z").replace("-", "")
        shutil.copy(geotiff, tmp_path / f"31UFS_7_2-{date}.tif")
    return tmp_path


@pytest.mark.parametrize("scheme", ["", "file:"])
def test_list_disk_geotiffs(geotiff_dir, scheme):
    assert list_disk_geotiffs(f"{scheme}{geotiff_dir}/*.tif") == [
        str(geotiff_dir / "openEO_2022-09-07Z.tif"),
        str(geotiff_dir / "openEO_2022-09-12Z.tif"),
        str(geotiff_dir / "openEO_2022-09-19Z.tif"),
    ]


def test_list_disk_geotiffs_unsupported_scheme():
    with pytest.raises(ValueError, match="Unsupported scheme"):
        list_disk_geotiffs("hdfs://data/*.tif")


@pytest.mark.parametrize(
    ["path", "expected"],
    [
        ("/data/assembled/31UDS_7_2-20190921.tif", "2019-09-21T00:00:00+00:00"),
        ("/data/s1_rtc_0446B9_S07E035_2021_02_03_MULTIBAND.tif", "2021-02-03T00:00:00+00:00"),
        ("results/tile_20210223.tif", "2021-02-23T00:00:00+00:00"),
    ],
)
def test_derive_date(path, expected):
    assert derive_date(path, r".+(\d{4})_?(\d{2})_?(\d{2}).*\.tif") == expected


def test_derive_date_no_match():
    with pytest.raises(ValueError, match="does not match"):
        derive_date("/data/no_date.tif", DATE_REGEX)


@pytest.mark.parametrize("recursive", [True, False])
def test_list_s3_geotiffs(recursive):
    jvm = mock.Mock()
    s3_client = getattr(getattr(jvm.org.openeo.geotrellis, "package$"), "MODULE$").s3Client.return_value
    keys = ["results/a/tile_20210223.tif", "results/a/tile_20210223.json", "results/b/tile_20210224.tif"]
    s3_objects = [mock.Mock(**{"key.return_value": key}) for key in keys]
    s3_client.listObjectsV2Paginator.return_value.contents.return_value.iterator.return_value = iter(s3_objects)
    request_builder = jvm.software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder.return_value

    uris = list_s3_geotiffs("s3://bucket/results", key_regex=r".+\.tif", recursive=recursive, jvm=jvm)

    assert uris == ["s3://bucket/results/a/tile_20210223.tif", "s3://bucket/results/b/tile_20210224.tif"]
    request_builder.bucket.assert_called_once_with("bucket")
    request_builder.bucket.return_value.prefix.assert_called_once_with("results")
    assert request_builder.bucket.return_value.prefix.return_value.delimiter.called != recursive


class TestLoadGeotiffs:
    @pytest.fixture
    def geotiffs(self, geotiff_dir):
        return [(path, derive_date(path, DATE_REGEX)) for path in list_disk_geotiffs(f"{geotiff_dir}/*.tif")]

    def _load(self, geotiffs, *, single_level: bool, from_date="2022-09-01T00:00:00+00:00", **kwargs):
        jvm = get_jvm()
        extent = jvm.geotrellis.vector.Extent(5.07, 51.21, 5.09, 51.22)
        projected_polygons = getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$").reproject(
            jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, "EPSG:4326"), 32631
        )
        datacube_params = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        getattr(datacube_params, "layoutScheme_$eq")("FloatingLayoutScheme")

        return load_geotiffs(
            jvm,
            collection_id="GEOTIFFS",
            geotiffs=geotiffs,
            band_names=["B02", "B03"],
            requested_band_names=["B03"],
            from_date=from_date,
            to_date="2022-09-19T00:00:00+00:00",
            single_level=single_level,
            extent=extent,
            extent_crs="EPSG:4326",
            projected_polygons=projected_polygons,
            datacube_params=datacube_params,
            metadata_properties=jvm.java.util.Collections.emptyMap(),
            correlation_id="",
            **kwargs,
        )

    @pytest.mark.parametrize(
        ["single_level", "expected_crs"],
        [
            (True, "EPSG:32631"),
            (False, "EPSG:3857"),
        ],
    )
    def test_load(self, geotiffs, single_level, expected_crs):
        pyramid = self._load(geotiffs, single_level=single_level)

        layer = pyramid.apply(0)._2()
        metadata = layer.metadata()
        assert metadata.crs().epsgCode().get() == int(expected_crs.split(":")[1])

        tiles = list(layer.collect())
        dates = sorted({tile._1().instant() for tile in tiles})
        assert [dt.datetime.fromtimestamp(d / 1000, tz=dt.timezone.utc).date().isoformat() for d in dates] == [
            "2022-09-07",
            "2022-09-12",
        ]  # end date is exclusive

        assert {tile._2().bandCount() for tile in tiles} == {1}

    def test_load_lat_lon_pyramid(self, geotiffs):
        pyramid = self._load(geotiffs, single_level=False, lat_lon=True, data_type="float32", nodata=0.0)

        metadata = pyramid.apply(0)._2().metadata()
        assert metadata.crs().epsgCode().get() == 4326
        assert metadata.cellType().name() == "float32ud0.0"

    def test_load_no_data(self, geotiffs):
        with pytest.raises(NoDataAvailableException):
            self._load(geotiffs, single_level=True, from_date="2022-09-19T00:00:00+00:00")


class TestLayerCatalog:
    def _catalog(self, data_source: dict):
        from openeogeotrellis.layercatalog import GeoPySparkLayerCatalog

        return GeoPySparkLayerCatalog(
            all_metadata=[
                {
                    "id": "GEOTIFFS",
                    "_vito": {"data_source": data_source},
                    "cube:dimensions": {
                        "x": {"type": "spatial", "axis": "x", "reference_system": 32631, "step": 10},
                        "y": {"type": "spatial", "axis": "y", "reference_system": 32631, "step": 10},
                        "t": {"type": "temporal"},
                        "bands": {"type": "bands", "values": ["B02", "B03"]},
                    },
                }
            ],
            vault=None,
        )

    def _load_params(self, bands=None):
        return LoadParameters(
            temporal_extent=["2022-09-10", "2022-09-30"],
            spatial_extent={"west": 5.07, "south": 51.21, "east": 5.09, "north": 51.22, "crs": "EPSG:4326"},
            bands=bands,
        )

    @staticmethod
    def _dates(cube) -> list:
        tiles = list(cube.get_max_level().srdd.rdd().collect())
        return sorted(
            {
                dt.datetime.fromtimestamp(tile._1().instant() / 1000, tz=dt.timezone.utc).date().isoformat()
                for tile in tiles
            }
        )

    @pytest.mark.parametrize("pyramid_levels", ["all", "highest"])
    def test_geotiff(self, geotiff_dir, pyramid_levels):
        catalog = self._catalog({"type": "geotiff", "glob_pattern": f"{geotiff_dir}/*.tif", "date_regex": DATE_REGEX})

        cube = catalog.load_collection(
            "GEOTIFFS", load_params=self._load_params(bands=["B03"]), env=EvalEnv({"pyramid_levels": pyramid_levels})
        )

        assert cube.metadata.band_names == ["B03"]
        assert self._dates(cube) == ["2022-09-12", "2022-09-19"]

    def _sentinel_hub_env(self, source_location: str, card4l: bool = False) -> EvalEnv:
        return EvalEnv(
            {
                "pyramid_levels": "highest",
                EVAL_ENV_KEY.DEPENDENCIES: [{"source_location": source_location, "card4l": card4l}],
            }
        )

    def test_sentinel_hub_dependency_on_disk(self, sentinel_hub_geotiff_dir):
        catalog = self._catalog({"type": "sentinel-hub"})
        env = self._sentinel_hub_env(f"file:{sentinel_hub_geotiff_dir}")

        cube = catalog.load_collection("GEOTIFFS", load_params=self._load_params(), env=env)

        assert cube.metadata.band_names == ["B02", "B03"]
        assert self._dates(cube) == ["2022-09-12", "2022-09-19"]
        assert cube.get_max_level().srdd.rdd().metadata().cellType().name() == "float32ud0.0"

    def test_sentinel_hub_dependency_on_s3(self, sentinel_hub_geotiff_dir):
        catalog = self._catalog({"type": "sentinel-hub"})
        env = self._sentinel_hub_env("s3://bucket/results")
        local_paths = list_disk_geotiffs(f"{sentinel_hub_geotiff_dir}/*.tif")
        s3_uris = [f"s3://bucket/results/{Path(path).name}" for path in local_paths]
        s3_to_local = dict(zip(s3_uris, local_paths))

        def load_local_geotiffs(jvm, *, geotiffs, **kwargs):
            return load_geotiffs(jvm, geotiffs=[(s3_to_local[uri], date) for uri, date in geotiffs], **kwargs)

        with (
            mock.patch(
                "openeogeotrellis.collections.geotiff_files.list_s3_geotiffs", return_value=s3_uris
            ) as list_s3_geotiffs_,
            mock.patch("openeogeotrellis.collections.geotiff_files.load_geotiffs", side_effect=load_local_geotiffs),
        ):
            cube = catalog.load_collection("GEOTIFFS", load_params=self._load_params(), env=env)

        list_s3_geotiffs_.assert_called_once_with(
            "s3://bucket/results", key_regex=r".+\.tif", recursive=True, jvm=mock.ANY
        )
        assert self._dates(cube) == ["2022-09-12", "2022-09-19"]
