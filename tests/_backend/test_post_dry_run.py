
from typing import Callable, List, Optional

import openeo_driver.ProcessGraphDeserializer
import pytest
from openeo_driver.backend import CollectionCatalog, OpenEoBackendImplementation
from openeo_driver.dry_run import DryRunDataTracer, SourceConstraint
from openeo_driver.ProcessGraphDeserializer import ENV_DRY_RUN_TRACER, ConcreteProcessing
from openeo_driver.testing import approxify
from openeo_driver.util.geometry import BoundingBox
from openeo_driver.utils import EvalEnv

from openeogeotrellis._backend.post_dry_run import (
    _align_extent,
    _buffer_extent,
    _extract_spatial_extent_from_constraint,
    _GridInfo,
    _snap_bbox,
    determine_global_extent,
)


def approxify_bbox(bbox: BoundingBox, rel=None, abs=1e-12) -> BoundingBox:
    # TODO: cleaner why to integration/use this? e.g. make it a BoundingBox method?
    return BoundingBox(
        west=pytest.approx(bbox.west, rel=rel, abs=abs),
        south=pytest.approx(bbox.south, rel=rel, abs=abs),
        east=pytest.approx(bbox.east, rel=rel, abs=abs),
        north=pytest.approx(bbox.north, rel=rel, abs=abs),
        crs=bbox.crs,
    )


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
    ) == approxify_bbox(BoundingBox(1.2, 2.8, 5.7, 8.4))

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
    assert aligned == approxify_bbox(BoundingBox(1.2, 3.3, 5.8, 8.1, crs="EPSG:4326"))


def test_align_extent_4326_no_target_resolution():
    extent = BoundingBox(1.222, 3.444, 5.666, 7.888, crs="EPSG:4326")
    source = _GridInfo(crs=4326, extent_x=(0, 10), extent_y=(0, 10), resolution=(0.2, 0.3))
    target = _GridInfo(crs=4326, resolution=None)
    aligned = _align_extent(extent=extent, source=source, target=target)
    assert aligned == approxify_bbox(BoundingBox(1.2, 3.3, 5.8, 8.1, crs="EPSG:4326"))


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
    ) == approxify_bbox(BoundingBox(500000 - 30, 5649824 - 30, 507016 + 30, 5660950 + 30, crs="EPSG:32631"), abs=1)


class DummyCatalog(CollectionCatalog):
    """
    Dummy collection catalog with helpers to easily set up  metadata relevant to the dry run (e.g. "cube:dimensions")
    """

    def define_collection_metadata(self, collection_id: str, **kwargs):
        """Helper to define a collection"""
        self._catalog[collection_id] = self.build_collection_metadata(collection_id, **kwargs)

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
        assert extents == (
            BoundingBox(1, 2, 3, 4, crs=4326),
            BoundingBox(1, 2, 3, 4, crs=4326),
        )

    @pytest.mark.parametrize(
        ["step_x", "step_y", "expected"],
        [
            # TODO: possible to improve numerical precision and avoid need for approx?
            (0.001, 0.001, approxify_bbox(BoundingBox(5.067, 51.213, 5.072, 5.988, crs=4326))),
            (0.005, 0.010, approxify_bbox(BoundingBox(5.065, 51.210, 5.075, 5.990, crs=4326))),
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
        assert extents == (BoundingBox(5.067891, 51.213456, 5.0712345, 5.9876543, crs=4326), expected)

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
        assert extents == (
            BoundingBox(5.067891, 51.213456, 5.0712345, 5.9876543, crs=4326),
            expected,
        )

    def test_determine_global_extent_minimal(self, dummy_catalog, extract_source_constraints):
        # Process graph with two load_collection sources with spatial extents
        pg = {
            "load_collection_1": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"west": 1, "south": 2, "east": 3, "north": 4},
                },
                "result": False,
            },
            "load_collection_2": {
                "process_id": "load_collection",
                "arguments": {
                    "id": "S2",
                    "spatial_extent": {"west": 1.5, "south": 2.5, "east": 3.5, "north": 4.5},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        global_extent = determine_global_extent(source_constraints=source_constraints, catalog=dummy_catalog)
        assert global_extent == {
            "global_extent_original": {"west": 1, "south": 2, "east": 3.5, "north": 4.5, "crs": "EPSG:4326"},
            "global_extent_aligned": {"west": 1, "south": 2, "east": 3.5, "north": 4.5, "crs": "EPSG:4326"},
        }

    def test_determine_global_extent_4326_millidegrees(self, dummy_catalog, extract_source_constraints):
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
        assert global_extent == {
            "global_extent_original": {
                "west": 11.123456,
                "south": 22.123456,
                "east": 66.123456,
                "north": 66.123456,
                "crs": "EPSG:4326",
            },
            "global_extent_aligned": approxify(
                {
                    "west": 11.123,
                    "south": 22.123,
                    "east": 66.124,
                    "north": 66.124,
                    "crs": "EPSG:4326",
                }
            ),
        }

    def test_extract_spatial_extent_from_constraint_load_stac_basic(
        self, dummy_catalog, extract_source_constraints, dummy_stac_api_server, dummy_stac_api
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
                    "proj:shape": [10, 10],
                }
            },
        )
        pg = {
            "load_collection": {
                "process_id": "load_stac",
                "arguments": {
                    "url": f"{dummy_stac_api}/collections/collection-1234",
                    "spatial_extent": {"west": 1.1234, "south": 2.1234, "east": 3.1234, "north": 4.1234},
                },
                "result": True,
            },
        }
        source_constraints = extract_source_constraints(pg)
        [source_constraint] = source_constraints
        extents = _extract_spatial_extent_from_constraint(source_constraint=source_constraint, catalog=dummy_catalog)
        assert extents == (
            BoundingBox(west=1.1234, south=2.1234, east=3.1234, north=4.1234, crs="EPSG:4326"),
            BoundingBox(west=1, south=2, east=3, north=4, crs="EPSG:4326"),
        )
