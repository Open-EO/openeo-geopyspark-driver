import collections
import dirty_equals

from typing import Callable, List, Optional

import openeo_driver.ProcessGraphDeserializer
import pytest
from openeo_driver.backend import CollectionCatalog, OpenEoBackendImplementation
from openeo_driver.dry_run import DryRunDataTracer, SourceConstraint
from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER, ConcreteProcessing
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv

from openeogeotrellis._backend.post_dry_run import (
    _align_extent,
    _buffer_extent,
    _determine_best_grid_from_proj_metadata,
    _extract_spatial_extent_from_constraint,
    _GridInfo,
    _snap_bbox,
    determine_global_extent,
    AlignedExtentResult,
)
from openeogeotrellis.load_stac import _ProjectionMetadata
from openeogeotrellis.util.caching import AlwaysCallWithoutCache


class TestGridInfo:
    def test_minimal(self):
        grid = _GridInfo(crs="EPSG:4326")
        assert grid.crs_raw == "EPSG:4326"
        assert grid.crs_epsg == 4326
        assert grid.resolution is None

    def test_more(self):
        grid = _GridInfo(
            crs="EPSG:32631",
            resolution=(10, 20),
            extent_x=(-1000, 1000),
            extent_y=(2000, 3000),
        )
        assert grid.crs_raw == "EPSG:32631"
        assert grid.crs_epsg == 32631
        assert grid.resolution == (10, 20)
        assert grid.extent_x == (-1000, 1000)
        assert grid.extent_y == (2000, 3000)

    def test_from_datacube_metadata(self):
        grid = _GridInfo.from_datacube_metadata(
            {
                "cube:dimensions": {
                    "t": {"type": "temporal", "extent": ["2020-01-01T00:00:00Z", "2020-12-31T23:59:59Z"]},
                    "x": {
                        "type": "spatial",
                        "axis": "x",
                        "reference_system": 32631,
                        "extent": [-20000, 30000],
                        "step": 10,
                    },
                    "y": {
                        "type": "spatial",
                        "axis": "y",
                        "reference_system": 32631,
                        "extent": [10000, 40000],
                        "step": 20,
                    },
                }
            }
        )
        assert grid.crs_raw == 32631
        assert grid.crs_epsg == 32631
        assert grid.resolution == (10, 20)
        assert grid.extent_x == (-20000, 30000)
        assert grid.extent_y == (10000, 40000)

    def test_from_datacube_metadata_missing_extent(self):
        grid = _GridInfo.from_datacube_metadata(
            {
                "cube:dimensions": {
                    "x": {"type": "spatial", "axis": "x", "extent": [2, 6]},
                    "y": {"type": "spatial", "axis": "y"},
                }
            }
        )
        assert grid.crs_raw == 4326
        assert grid.crs_epsg == 4326
        assert grid.resolution is None
        assert grid.extent_x == (2, 6)
        assert grid.extent_y is None

    def test_eq(self):
        assert _GridInfo(crs=4326) == _GridInfo(crs=4326)
        assert _GridInfo(crs=4326) != _GridInfo(crs=32631)

        assert _GridInfo(crs=4326, resolution=(0.01, 0.02)) != _GridInfo(crs=4326)
        assert _GridInfo(crs=4326, resolution=(0.01, 0.02)) == _GridInfo(crs=4326, resolution=(0.01, 0.02))
        assert _GridInfo(crs=4326, resolution=(0.01, 0.02)) == _GridInfo(crs=4326, resolution=[0.01, 0.02])

        assert _GridInfo(
            crs=4326,
            resolution=(0.01, 0.02),
            extent_x=(10, 30),
            extent_y=(20, 40),
        ) != _GridInfo(
            crs=4326,
            resolution=(0.01, 0.02),
        )
        assert _GridInfo(
            crs=4326,
            resolution=(0.01, 0.02),
            extent_x=(10, 30),
            extent_y=(20, 40),
        ) == _GridInfo(
            crs=4326,
            resolution=[0.01, 0.02],
            extent_x=[10, 30],
            extent_y=[20, 40],
        )

    def test_as_cache_key(self):
        grid1 = _GridInfo(crs="EPSG:4326")
        grid2 = _GridInfo(crs="EPSG:4326")
        grid3 = _GridInfo(crs="EPSG:32631", resolution=(10, 20))
        grid4 = _GridInfo(crs="EPSG:32631", resolution=(10, 20))
        grid5 = _GridInfo(crs="EPSG:32631", resolution=(20, 10))

        cache = {grid1: 1, grid3: 3}
        assert cache[grid2] == 1
        assert cache[grid4] == 3
        assert grid5 not in cache


def test_snap_bbox():
    bbox = BoundingBox(1.222, 3.444, 5.666, 7.888)

    assert _snap_bbox(
        bbox,
        resolution=(1, 1),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == BoundingBox(1, 3, 6, 8)

    assert _snap_bbox(
        bbox,
        resolution=(2, 3),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == BoundingBox(0, 3, 6, 9)

    assert _snap_bbox(
        bbox,
        resolution=(0.3, 0.7),
        extent_x=(0, 10),
        extent_y=(0, 10),
    ) == (BoundingBox(1.2, 2.8, 5.7, 8.4).approx(abs=1e-6))

    assert _snap_bbox(
        bbox,
        resolution=(0.3, 0.7),
        extent_x=(1.1, 10),
        extent_y=(2.1, 5),
    ) == BoundingBox(1.1, 2.8, 5.9, 5)


def test_align_extent_4326_basic():
    extent = BoundingBox(1.222, 3.444, 5.666, 7.888, crs="EPSG:4326")
    source = _GridInfo(crs=4326, extent_x=(0, 10), extent_y=(0, 10), resolution=(0.2, 0.3))
    target = _GridInfo(crs=4326, resolution=(0.2, 0.3))
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(1.2, 3.3, 5.8, 8.1, crs="EPSG:4326").approx(abs=1e-6)


def test_align_extent_4326_no_target_resolution():
    extent = BoundingBox(1.222, 3.444, 5.666, 7.888, crs="EPSG:4326")
    source = _GridInfo(crs=4326, extent_x=(0, 10), extent_y=(0, 10), resolution=(0.2, 0.3))
    target = _GridInfo(crs=4326, resolution=None)
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(1.2, 3.3, 5.8, 8.1, crs="EPSG:4326").approx(abs=1e-6)


def test_align_extent_auto_utm():
    extent = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")
    source = _GridInfo(crs="AUTO:42001", resolution=(10, 10))
    target = _GridInfo(crs="EPSG:32631", resolution=(10, 10))
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == BoundingBox(west=500000, south=5649820, east=507020, north=5660960, crs="EPSG:32631")


def test_buffer_extent_stay_in_4326():
    bbox = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")

    assert _buffer_extent(
        bbox, buffer=(10, 10), sampling=_GridInfo(crs="EPSG:4326", resolution=(0.01, 0.01))
    ) == BoundingBox(2.9, 50.9, 3.2, 51.2, crs="EPSG:4326")

    assert _buffer_extent(bbox, buffer=3, sampling=_GridInfo(crs="EPSG:4326", resolution=(0.01, 0.01))) == BoundingBox(
        2.97, 50.97, 3.13, 51.13, crs="EPSG:4326"
    )

    assert _buffer_extent(
        bbox, buffer=(3, 7), sampling=_GridInfo(crs="EPSG:4326", resolution=(0.01, 0.02))
    ) == BoundingBox(2.97, 50.86, 3.13, 51.24, crs="EPSG:4326")


def test_buffer_extent_4326_to_utm():
    bbox = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")

    assert _buffer_extent(
        bbox, buffer=(3, 3), sampling=_GridInfo(crs="EPSG:32631", resolution=(10, 10))
    ) == BoundingBox(500000 - 30, 5649824 - 30, 507016 + 30, 5660950 + 30, crs="EPSG:32631").approx(abs=1)


def test_buffer_extent_4326_to_auto_utm():
    bbox = BoundingBox(3, 51, 3.1, 51.1, crs="EPSG:4326")
    assert _buffer_extent(
        bbox, buffer=(3, 3), sampling=_GridInfo(crs="AUTO:42001", resolution=(10, 10))
    ) == BoundingBox(500000 - 30, 5649824 - 30, 507016 + 30, 5660950 + 30, crs="EPSG:32631").approx(abs=1)


class DummyCatalog(CollectionCatalog):
    """
    Dummy collection catalog with helpers to easily set up  metadata relevant to the dry run (e.g. "cube:dimensions")
    """

    def define_collection_metadata(
        self, collection_id: str, *, metadata: Optional[dict] = None, cube_dimensions: Optional[dict] = None
    ):
        """Helper to define a collection"""
        self._catalog[collection_id] = self.build_collection_metadata(
            collection_id=collection_id, metadata=metadata, cube_dimensions=cube_dimensions
        )

    @classmethod
    def build_collection_metadata(
        cls, collection_id: str, *, metadata: Optional[dict] = None, cube_dimensions: Optional[dict] = None
    ):
        """Helper to create collection metadata dict"""
        return {
            **{
                "id": collection_id,
                "cube:dimensions": cls.build_cube_dimensions(**(cube_dimensions or {})),
            },
            **(metadata or {}),
        }

    @classmethod
    def build_cube_dimensions(
        cls, x: Optional[dict] = None, y: Optional[dict] = None, t: Optional[dict] = None
    ) -> dict:
        """Helper to create a "cube:dimensions" dict"""
        # merging some defaults with given overrides.
        return {
            "x": {**{"type": "spatial", "axis": "x", "reference_system": 4326, "extent": [-180, 180]}, **(x or {})},
            "y": {**{"type": "spatial", "axis": "y", "reference_system": 4326, "extent": [-90, 90]}, **(y or {})},
            "t": {**{"type": "temporal", "extent": ["2025-01-01T00:00:00Z", "2025-12-31T23:59:59Z"]}, **(t or {})},
            # TODO: bands too?
        }


# TODO: this projjson dump is a naive copy-paste from some layercatalog.json.
#       Is there a better way to produce this? Are there other, more generic variants to consider too?
REFERENCE_SYSTEM_AUTO_UTM_PROJJSON = {
    "$schema": "https://proj.org/schemas/v0.2/projjson.schema.json",
    "type": "GeodeticCRS",
    "id": {"authority": "OGC", "version": "1.3", "code": "Auto42001"},
    "name": "AUTO 42001 (Universal Transverse Mercator)",
    "datum": {
        "type": "GeodeticReferenceFrame",
        "name": "World Geodetic System 1984",
        "ellipsoid": {"name": "WGS 84", "semi_major_axis": 6378137, "inverse_flattening": 298.257223563},
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {"name": "Geodetic latitude", "abbreviation": "Lat", "direction": "north", "unit": "degree"},
            {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east", "unit": "degree"},
        ],
    },
    "area": "World",
    "bbox": {"south_latitude": -90, "west_longitude": -180, "north_latitude": 90, "east_longitude": 180},
}


class TestPostDryRun:
    @pytest.fixture
    def dummy_catalog(self) -> DummyCatalog:
        # Predefine a simple collection with default cube:dimensions metadata
        return DummyCatalog([DummyCatalog.build_collection_metadata(collection_id="S2")])

    @pytest.fixture
    def backend_implementation(self, dummy_catalog) -> OpenEoBackendImplementation:
        return OpenEoBackendImplementation(catalog=dummy_catalog, processing=ConcreteProcessing())

    @pytest.fixture
    def extract_source_constraints(self, backend_implementation) -> Callable[[dict], List[SourceConstraint]]:
        """Fixture for a function that extracts source constraints from a process graph using dry-run tracing"""

        def extract(pg: dict) -> List[SourceConstraint]:
            dry_run_tracer = DryRunDataTracer()
            env = EvalEnv(
                {
                    ENV_DRY_RUN_TRACER: dry_run_tracer,
                    "backend_implementation": backend_implementation,
                }
            )
            openeo_driver.ProcessGraphDeserializer.evaluate(pg, env=env)
            return dry_run_tracer.get_source_constraints()

        return extract

    def test_extract_spatial_extent_from_constraint_load_collection_empty(
        self, dummy_catalog, extract_source_constraints
    ):
        # Process graph without spatial extent
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {"id": "S2"},
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents is None

    def test_extract_spatial_extent_from_constraint_load_collection_minimal(
        self, dummy_catalog, extract_source_constraints
    ):
        # Just bare essentials (just extent extraction, no alignment/buffering)
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=BoundingBox(1, 2, 3, 4, crs=4326),
            variants={
                "original": BoundingBox(1, 2, 3, 4, crs=4326),
                "target_aligned": BoundingBox(1, 2, 3, 4, crs=4326),
            },
        )

    @pytest.mark.parametrize(
        ["step_x", "step_y", "expected"],
        [
            # TODO: possible to improve numerical precision and avoid need for approx?
            (0.001, 0.001, BoundingBox(5.067, 51.213, 5.072, 5.988, crs=4326).approx(abs=1e-6)),
            (0.005, 0.010, BoundingBox(5.065, 51.210, 5.075, 5.990, crs=4326).approx(abs=1e-6)),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_collection_4236_millidegrees(
        self, dummy_catalog, extract_source_constraints, step_x, step_y, expected
    ):
        # LonLat collection with millidegree resolution
        dummy_catalog.define_collection_metadata(
            "S123-millidegree", cube_dimensions={"x": {"step": step_x}, "y": {"step": step_y}}
        )
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S123-millidegree",
                    "spatial_extent": {"west": 5.067891, "south": 51.213456, "east": 5.0712345, "north": 5.9876543},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": BoundingBox(5.067891, 51.213456, 5.0712345, 5.9876543, crs=4326),
                "target_aligned": expected,
            },
        )

    @pytest.mark.parametrize(
        ["step_x", "step_y", "expected"],
        [
            (10, 10, BoundingBox(west=644420, south=662270, east=729280, north=5675610, crs="EPSG:32631")),
            (11, 13, BoundingBox(west=644424, south=662259, east=729278, north=5675605, crs="EPSG:32631")),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_collection_auto_utm_alignment(
        self, dummy_catalog, extract_source_constraints, step_x, step_y, expected
    ):
        # Auto-UTM
        dummy_catalog.define_collection_metadata(
            collection_id="S123-UTM",
            cube_dimensions={
                "x": {
                    "reference_system": REFERENCE_SYSTEM_AUTO_UTM_PROJJSON,
                    "extent": [166_000, 834_000],
                    "step": step_x,
                },
                "y": {
                    "reference_system": REFERENCE_SYSTEM_AUTO_UTM_PROJJSON,
                    "extent": [0, 10_000_000],
                    "step": step_y,
                },
            },
        )
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S123-UTM",
                    "spatial_extent": {"west": 5.067891, "south": 51.213456, "east": 5.0712345, "north": 5.9876543},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": BoundingBox(5.067891, 51.213456, 5.0712345, 5.9876543, crs=4326),
                "target_aligned": expected,
            },
        )

    @pytest.mark.parametrize(
        ["resolution", "expected"],
        [
            (100, BoundingBox(west=644400, south=662200, east=729300, north=5675700, crs="EPSG:32631")),
            (1000, BoundingBox(west=644000, south=662000, east=730000, north=5676000, crs="EPSG:32631")),
            ((70, 700), BoundingBox(west=644420, south=662200, east=729330, north=5676300, crs="EPSG:32631")),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_collection_resample(
        self, dummy_catalog, extract_source_constraints, resolution, expected
    ):
        dummy_catalog.define_collection_metadata(
            collection_id="S123-UTM",
            cube_dimensions={
                "x": {
                    "reference_system": REFERENCE_SYSTEM_AUTO_UTM_PROJJSON,
                    "extent": [166_000, 834_000],
                    "step": 10,
                },
                "y": {
                    "reference_system": REFERENCE_SYSTEM_AUTO_UTM_PROJJSON,
                    "extent": [0, 10_000_000],
                    "step": 10,
                },
            },
        )
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S123-UTM",
                    "spatial_extent": {"west": 5.067891, "south": 51.213456, "east": 5.0712345, "north": 5.9876543},
                },
            },
            "resample": {
                "process_id": "resample_spatial",
                "arguments": {
                    "data": {"from_node": "load_collection"},
                    "projection": None,
                    "resolution": resolution,
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": BoundingBox(5.067891, 51.213456, 5.0712345, 5.9876543, crs=4326),
                "target_aligned": expected,
            },
        )
    def test_determine_global_extent_load_collection_minimal(self, dummy_catalog, extract_source_constraints):
        # Process graph with two load_collection sources with spatial extents
        pg = {
            "lc1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
                },
            },
            "lc2": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"west": 1.5, "south": 2.5, "east": 3.5, "north": 4.5},
                },
            },
            "merge": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "lc1"}, "cube2": {"from_node": "lc2"}},
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        global_extent = determine_global_extent(source_constraints=source_constraints, catalog=dummy_catalog)
        assert global_extent == {
            "global_extent": BoundingBox(1, 2, 3.5, 4.5, crs="EPSG:4326"),
            "global_extent_variants": {
                "original": BoundingBox(1, 2, 3.5, 4.5, crs="EPSG:4326"),
                "target_aligned": BoundingBox(1, 2, 3.5, 4.5, crs="EPSG:4326"),
            },
        }

    def test_determine_global_extent_load_collection_4326_millidegrees(self, dummy_catalog, extract_source_constraints):
        dummy_catalog.define_collection_metadata(
            "S123-millidegree", cube_dimensions={"x": {"step": 0.001}, "y": {"step": 0.001}}
        )
        pg = {
            "load_collection_1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S123-millidegree",
                    "spatial_extent": {"west": 11.123456, "south": 22.123456, "east": 33.123456, "north": 44.123456},
                },
                "result": False,
            },
            "load_collection_2": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S123-millidegree",
                    "spatial_extent": {"west": 55.123456, "south": 55.123456, "east": 66.123456, "north": 66.123456},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        global_extent = determine_global_extent(source_constraints=source_constraints, catalog=dummy_catalog)

        expected_orig = BoundingBox(
            11.123456,
            22.123456,
            66.123456,
            66.123456,
            crs="EPSG:4326",
        )
        expected_aligned = BoundingBox(
            11.123,
            22.123,
            66.124,
            66.124,
            crs="EPSG:4326",
        ).approx(abs=1e-6)
        assert global_extent == {
            "global_extent": expected_aligned,
            "global_extent_variants": {
                "original": expected_orig,
                "target_aligned": expected_aligned,
            },
        }

    @pytest.mark.parametrize(
        ["shift", "proj_shape", "expected"],
        [
            (0.1234, [10, 10], BoundingBox(1.0, 2.0, 3.0, 4.0, crs=4326)),
            (0.5678, [10, 10], BoundingBox(1.4, 2.4, 3.0, 4.0, crs=4326)),
            (0.5678, [200, 200], BoundingBox(1.56, 2.56, 3.0, 4.0, crs=4326)),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_stac_basic(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
        shift,
        proj_shape,
        expected,
    ):
        """STAC collection with single item and single asset -> extract native footprint from proj metadata."""
        dummy_stac_api_server.define_collection("collection-1234")
        dummy_stac_api_server.define_item(
            collection_id="collection-1234",
            item_id="item-1",
            bbox=[1, 2, 3, 4],
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "proj:code": 4326,
                    "proj:bbox": [1, 2, 3, 4],
                    "proj:shape": proj_shape,
                }
            },
        )
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 1 + shift, "south": 2 + shift, "east": 3 + shift, "north": 4 + shift},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": BoundingBox(
                    west=1 + shift, south=2 + shift, east=3 + shift, north=4 + shift, crs="EPSG:4326"
                ),
                "assets_full_bbox": BoundingBox(west=1, south=2, east=3, north=4, crs="EPSG:4326"),
                "assets_covered_bbox": expected,
            },
        )

    def test_extract_spatial_extent_from_constraint_load_stac_issue_1299_3569441306(
        self, dummy_catalog, extract_source_constraints, dummy_stac_api_server, dummy_stac_api
    ):
        """
        use case from https://github.com/Open-EO/openeo-geopyspark-driver/issues/1299#issuecomment-3569441306
        """
        stac_collection_id = "terrascope-s5p-l3-ch4-td-v2"
        dummy_stac_api_server.define_collection(stac_collection_id)
        dummy_stac_api_server.define_item(
            collection_id=stac_collection_id,
            item_id="S5P_OFFL_L3_CH4_TD_20240602_V200",
            bbox=[-180.0, -89.0, 180.0, 89.0],
            properties={
                "datetime": "2024-06-02T06:48:16Z",
                "proj:code": "EPSG:4326",
                "proj:bbox": [-180.0, -89.0, 180.0, 89.0],
                "proj:shape": [3560, 7200],
                "proj:transform": [0.05, 0.0, -180.0, 0.0, -0.05, 89.0, 0.0, 0.0, 1.0],
            },
            assets={
                "CH4": {
                    "href": "https://stac.test/data/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "bands": [{"name": "CH4"}],
                }
            },
        )
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/{stac_collection_id}",
                    "bands": ["CH4"],
                    "spatial_extent": {
                        "crs": "EPSG:4326",
                        "east": 6.5154,
                        "north": 51.5678,
                        "south": 49.51234,
                        "west": 2.54578,
                    },
                    "temporal_extent": ["2024-06-02", "2024-06-09"],
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        expected_aligned = BoundingBox(2.5, 49.5, 6.55, 51.60, crs="EPSG:4326").approx(abs=1e-6)
        assert extents == AlignedExtentResult(
            extent=expected_aligned,
            variants={
                "original": BoundingBox(2.54578, 49.51234, 6.5154, 51.5678, crs="EPSG:4326"),
                "assets_full_bbox": BoundingBox(-180.0, -89.0, 180.0, 89.0, crs="EPSG:4326"),
                "assets_covered_bbox": expected_aligned,
            },
        )

    @pytest.mark.parametrize(
        ["resample_resolution", "expected"],
        [
            (10, BoundingBox(west=647300, south=5665690, east=697580, north=5711820, crs="EPSG:32631")),
            ((10, 10), BoundingBox(west=647300, south=5665690, east=697580, north=5711820, crs="EPSG:32631")),
            ((1000, 500), BoundingBox(west=647000, south=5665500, east=698000, north=5712000, crs="EPSG:32631")),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_stac_with_resample_spatial(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
        resample_resolution,
        expected,
    ):
        """STAC collection with single item and single asset -> extract native footprint from proj metadata."""
        dummy_stac_api_server.define_collection("collection-1234")
        dummy_stac_api_server.define_item(
            collection_id="collection-1234",
            item_id="item-1",
            bbox=[5, 51, 6, 52],
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "proj:code": 4326,
                    "proj:bbox": [5, 51, 6, 52],
                    "proj:shape": [1000, 1000],
                }
            },
        )
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 5.1234, "south": 51.1234, "east": 5.8234, "north": 51.5234},
                },
            },
            "resample": {
                "process_id": "resample_spatial",
                "arguments": {
                    "data": {"from_node": "load_stac"},
                    "resolution": resample_resolution,
                    "projection": 32631,
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": BoundingBox(west=5.1234, south=51.1234, east=5.8234, north=51.5234, crs="EPSG:4326"),
                "assets_covered_bbox": BoundingBox(west=5.123, south=51.123, east=5.824, north=51.524, crs="EPSG:4326"),
                "assets_full_bbox": BoundingBox(west=5, south=51, east=6, north=52, crs="EPSG:4326"),
                "resampled": expected,
            },
        )

    @pytest.mark.parametrize(
        ["kernel_size", "expected"],
        [
            (
                # Base case: no kernel, no pixel buffer
                None,
                BoundingBox(west=5.12, south=51.12, east=5.83, north=51.53, crs="EPSG:4326"),
            ),
            (
                # Kernel size 5 -> buffer size 2 (TODO: current implementation gives 3)
                5,
                BoundingBox(west=5.09, south=51.09, east=5.86, north=51.56, crs="EPSG:4326").approx(abs=1e-6),
            ),
            (
                # Kernel size 21 -> buffer size 10 (TODO: current implementation gives 11)
                21,
                BoundingBox(west=5.01, south=51.01, east=5.94, north=51.64, crs="EPSG:4326"),
            ),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_stac_with_pixel_buffer(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
        kernel_size,
        expected,
    ):
        """STAC collection with single item and single asset -> extract native footprint from proj metadata."""
        dummy_stac_api_server.define_collection("collection-1234")
        dummy_stac_api_server.define_item(
            collection_id="collection-1234",
            item_id="item-1",
            bbox=[5, 51, 6, 52],
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "proj:code": 4326,
                    "proj:bbox": [5, 51, 6, 52],
                    "proj:shape": [100, 100],
                }
            },
        )
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 5.1234, "south": 51.1234, "east": 5.8234, "north": 51.5234},
                },
                "result": True,
            },
        }
        if kernel_size:
            pg["load_stac"]["result"] = False
            pg["kernel"] = {
                "process_id": "apply_kernel",
                "arguments": {
                    "data": {"from_node": "load_stac"},
                    "kernel": [[1] * kernel_size] * kernel_size,
                },
                "result": True,
            }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)

        if kernel_size:
            expected_variants = {
                "original": BoundingBox(5.1234, 51.1234, 5.8234, 51.5234, crs="EPSG:4326"),
                "assets_full_bbox": BoundingBox(5, 51, 6, 52, crs="EPSG:4326"),
                "assets_covered_bbox": BoundingBox(5.12, 51.12, 5.83, 51.53, crs="EPSG:4326"),
                "pixel_buffered": expected,
            }
        else:
            expected_variants = {
                "original": BoundingBox(5.1234, 51.1234, 5.8234, 51.5234, crs="EPSG:4326"),
                "assets_full_bbox": BoundingBox(5, 51, 6, 52, crs="EPSG:4326"),
                "assets_covered_bbox": expected,
            }
        assert extents == AlignedExtentResult(
            extent=expected,
            variants=expected_variants,
        )

    def test_extract_spatial_extent_from_constraint_load_stac_no_given_extent(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
    ):
        """
        Use case from https://github.com/Open-EO/openeo-geopyspark-driver/issues/648
        """
        dummy_stac_api_server.define_collection("collection-1234")
        dummy_stac_api_server.define_item(
            collection_id="collection-1234",
            item_id="item-1",
            bbox=[1, 2, 3, 4],
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "proj:epsg": 32632,
                    "proj:bbox": [631800, 5167700, 655800, 5184200],
                    "proj:shape": [33, 48],
                }
            },
        )
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {"url": f"{dummy_stac_api}/collections/collection-1234"},
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=BoundingBox(631800, 5167700, 655800, 5184200, crs="EPSG:32632"),
            variants={
                "original": None,
                "assets_covered_bbox": None,
                "assets_full_bbox": BoundingBox(
                    west=631800, south=5167700, east=655800, north=5184200, crs="EPSG:32632"
                ),
            },
        )

    def test_extract_spatial_extent_from_constraint_load_stac_no_data(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
    ):
        """
        pixel_buffer usage (e.g. apply_kernel), but no data discover projection data from
        https://github.com/Open-EO/openeo-geopyspark-driver/issues/1563
        """
        dummy_stac_api_server.define_collection("collection-1234")
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
                },
            },
            "apply_kernel": {
                "process_id": "apply_kernel",
                "arguments": {
                    "data": {"from_node": "load_stac"},
                    "kernel": [[1] * 3] * 3,
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=BoundingBox(west=1, south=2, east=3, north=4, crs="EPSG:4326"),
            variants={
                "original": BoundingBox(west=1, south=2, east=3, north=4, crs="EPSG:4326"),
                "assets_full_bbox": None,
                "assets_covered_bbox": None,
            },
        )

    def test_determine_global_extent_load_stac_minimal(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
    ):
        dummy_stac_api_server.define_collection("collection-1234")
        dummy_stac_api_server.define_item(
            collection_id="collection-1234",
            item_id="item-1",
            bbox=[1, 2, 3, 4],
            assets={
                "asset-1": {
                    "href": "https://stac.test/asset-1.tif",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                    "proj:code": 4326,
                    "proj:bbox": [1, 2, 3, 4],
                    "proj:shape": [200, 200],
                }
            },
        )
        pg = {
            "ls1": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 1.234, "south": 2.345, "east": 1.789, "north": 2.891},
                },
            },
            "ls2": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 2.234, "south": 3.345, "east": 2.789, "north": 3.891},
                },
            },
            "merge": {
                "process_id": "merge_cubes",
                "arguments": {"cube1": {"from_node": "ls1"}, "cube2": {"from_node": "ls2"}},
                "result": True,
            },
        }

        source_constraints = extract_source_constraints(pg)
        global_extent = determine_global_extent(source_constraints=source_constraints, catalog=dummy_catalog)
        expected = BoundingBox(1.23, 2.34, 2.79, 3.90, crs="EPSG:4326").approx(abs=1e-6)
        assert global_extent == {
            "global_extent": expected,
            "global_extent_variants": {
                "original": BoundingBox(1.234, 2.345, 2.789, 3.891, crs="EPSG:4326"),
                "assets_full_bbox": BoundingBox(1, 2, 3, 4, crs="EPSG:4326"),
                "assets_covered_bbox": expected,
            },
        }

    @pytest.mark.parametrize(
        ["cache", "expected_searches"],
        [
            (None, 1),  # cache=None: use default caching
            (AlwaysCallWithoutCache(), 3),
        ],
    )
    def test_determine_global_extent_load_stac_caching(
        self,
        dummy_catalog,
        extract_source_constraints,
        dummy_stac_api_server,
        dummy_stac_api,
        cache,
        expected_searches,
    ):
        # Multiple source constraints with same essentials,
        # and some differences in non-essential details
        source_constraints = [
            (
                ("load_stac", (f"{dummy_stac_api}/collections/collection-123", (), ("B02",))),
                {"bands": ["B02"], "breakfast": "cereal"},
            ),
            (
                ("load_stac", (f"{dummy_stac_api}/collections/collection-123", (), ("B02",))),
                {"bands": ["B02"], "shoe:size": 42},
            ),
            (
                ("load_stac", (f"{dummy_stac_api}/collections/collection-123", (), ("B02",))),
                {"bands": ["B02"], "pets": ["dog"]},
            ),
        ]
        global_extent = determine_global_extent(
            source_constraints=source_constraints, catalog=dummy_catalog, cache=cache
        )
        expected = BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326").approx(abs=1e-6)
        assert global_extent == {
            "global_extent": expected,
            "global_extent_variants": {
                "assets_full_bbox": expected,
            },
        }
        # Check request history histogram by path for search requests
        assert collections.Counter(
            (r["method"], r["path"]) for r in dummy_stac_api_server.request_history
        ) == dirty_equals.IsPartialDict({("GET", "/search"): expected_searches})

    def test_extract_spatial_extent_from_constraint_load_collection_type_stac_minimal(
        self, dummy_catalog, extract_source_constraints, dummy_stac_api
    ):
        """load_collection of a STAC based collection as is, without any spatiotemporal filtering"""
        stac_url = f"{dummy_stac_api}/collections/collection-123"
        dummy_catalog.define_collection_metadata(
            collection_id="STAC123",
            metadata={"_vito": {"data_source": {"type": "stac", "url": stac_url}}},
        )
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {"id": "STAC123"},
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326"),
            variants={
                "original": None,
                "assets_full_bbox": BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326"),
                "assets_covered_bbox": None,
            },
        )

    @pytest.mark.parametrize(
        ["load_collection_args", "expected_extents"],
        [
            (  # base case: no spatiotemporal filtering
                {},
                AlignedExtentResult(
                    extent=BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326"),
                    variants={
                        "original": None,
                        "assets_full_bbox": BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326"),
                        "assets_covered_bbox": None,
                    },
                ),
            ),
            (  # Spatial extent that limits bboxes
                {"spatial_extent": {"west": 2.5, "south": 49.5, "east": 6, "north": 51.5}},
                AlignedExtentResult(
                    extent=BoundingBox(west=2.5, south=49.5, east=6, north=51.5, crs="EPSG:4326"),
                    variants={
                        "original": BoundingBox(west=2.5, south=49.5, east=6, north=51.5, crs="EPSG:4326"),
                        "assets_full_bbox": BoundingBox(west=2, south=49, east=7, north=52, crs="EPSG:4326"),
                        "assets_covered_bbox": BoundingBox(west=2.5, south=49.5, east=6, north=51.5, crs="EPSG:4326"),
                    },
                ),
            ),
            (  # Temporal extent that limits assets of collection-123
                {"temporal_extent": ["2024-06-01", "2024-07-10"]},
                AlignedExtentResult(
                    extent=BoundingBox(west=3, south=50, east=7, north=52, crs="EPSG:4326"),
                    variants={
                        "original": None,
                        "assets_full_bbox": BoundingBox(west=3, south=50, east=7, north=52, crs="EPSG:4326"),
                        "assets_covered_bbox": None,
                    },
                ),
            ),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_collection_type_stac_spatiotemporal_filtering(
        self, dummy_catalog, extract_source_constraints, dummy_stac_api, load_collection_args, expected_extents
    ):
        """load_collection of a STAC based collection with additional (spatiotemporal) filtering"""
        stac_url = f"{dummy_stac_api}/collections/collection-123"
        dummy_catalog.define_collection_metadata(
            collection_id="STAC123",
            metadata={"_vito": {"data_source": {"type": "stac", "url": stac_url}}},
        )
        pg = {
            "load_collection": {
                "process_id": "load_collection",
                "arguments": {"id": "STAC123", **load_collection_args},
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == expected_extents

    @pytest.mark.parametrize(
        ["flavor", "expected"],
        [
            (None, BoundingBox(2, 49, 7, 52, crs="EPSG:4326")),
            ("apple", BoundingBox(2, 49, 3, 50, crs="EPSG:4326")),
            ("banana", BoundingBox(3, 50, 5, 51, crs="EPSG:4326")),
        ],
    )
    def test_extract_spatial_extent_from_constraint_load_stac_with_property_filtering(
        self, dummy_catalog, extract_source_constraints, dummy_stac_api_server, dummy_stac_api, flavor, expected
    ):
        if flavor:
            property_filters = {
                "flavor": {
                    "process_graph": {
                        "eq": {
                            "process_id": "eq",
                            "arguments": {"x": {"from_parameter": "value"}, "y": flavor},
                            "result": True,
                        }
                    }
                }
            }
        else:
            property_filters = {}
        pg = {
            "load_stac": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-123",
                    "properties": property_filters,
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == AlignedExtentResult(
            extent=expected,
            variants={
                "original": None,
                "assets_full_bbox": expected,
                "assets_covered_bbox": None,
            },
        )

class TestDetermineBestGridFromProjMetadata:
    def test_empty(self):
        assert _determine_best_grid_from_proj_metadata([]) is None
        assert _determine_best_grid_from_proj_metadata([_ProjectionMetadata()]) is None

    def test_simple_crs(self):
        grid = _determine_best_grid_from_proj_metadata([_ProjectionMetadata(epsg=4326)])
        assert grid == _GridInfo(crs="EPSG:4326", resolution=None)

    def test_most_common_crs(self):
        grid = _determine_best_grid_from_proj_metadata(
            [
                _ProjectionMetadata(epsg=32632),
                _ProjectionMetadata(epsg=32631),
                _ProjectionMetadata(),
                _ProjectionMetadata(epsg=32631),
            ]
        )
        assert grid == _GridInfo(crs="EPSG:32631")

    def test_most_common_crs_except_none(self):
        grid = _determine_best_grid_from_proj_metadata(
            [
                _ProjectionMetadata(),
                _ProjectionMetadata(epsg=32631),
                _ProjectionMetadata(),
                _ProjectionMetadata(),
            ]
        )
        assert grid == _GridInfo(crs="EPSG:32631")

    def test_most_common_crs_with_resolution(self):
        proj_bbox = (600_000, 5_000_000, 700_000, 5_100_000)
        proj_shape = (10_000, 10_000)
        grid = _determine_best_grid_from_proj_metadata(
            [
                _ProjectionMetadata(epsg=32632, bbox=proj_bbox, shape=proj_shape),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape),
                _ProjectionMetadata(),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape),
            ]
        )
        assert grid == _GridInfo(crs="EPSG:32631", resolution=(10, 10))

    @pytest.mark.parametrize(
        ["bbox_jitter", "expected_resolution"],
        [
            # Small imprecision: still pick 10 as resolution
            (0.1, (10, 10)),
            # Error is too large: resolution 1000 wins
            (100, (1000, 1000)),
        ],
    )
    def test_most_common_crs_with_resolution_precision(self, bbox_jitter, expected_resolution):
        """
        Resolution picking should be robust against small numerical imprecisions
        """
        proj_bbox = (600_000, 5_000_000, 700_000, 5_100_000)
        # Introduce small imprecision of calculated resolution in a couple of assets
        proj_bbox_off = (600_000, 5_000_000, 700_000 + bbox_jitter, 5_100_000 + bbox_jitter)
        proj_shape = (10_000, 10_000)
        proj_shape_low_res = (100, 100)
        grid = _determine_best_grid_from_proj_metadata(
            [
                # 3 cases with resolution (10, 10)
                # 4 cases with (10000, 10000)
                # 2 cases with (10, 10) + imprecision:
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox_off, shape=proj_shape),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape_low_res),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape_low_res),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape_low_res),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape_low_res),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox, shape=proj_shape),
                _ProjectionMetadata(epsg=32631, bbox=proj_bbox_off, shape=proj_shape),
            ]
        )
        assert grid == _GridInfo(crs="EPSG:32631", resolution=expected_resolution)
