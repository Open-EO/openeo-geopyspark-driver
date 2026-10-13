"""
Loading of timestamped GeoTIFF files (on disk or on S3) as a data cube,
through `org.openeo.geotrellis.file.PyramidFactory`.
"""

import datetime as dt
import glob
import logging
import os
import re
from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from openeo_driver.util.geometry import reproject_bounding_box

from openeogeotrellis.load_stac import NoDataAvailableException
from openeogeotrellis.utils import reproject_cellsize

logger = logging.getLogger(__name__)

# (uri, ISO 8601 nominal date)
TimestampedGeoTiff = Tuple[str, str]


def list_disk_geotiffs(glob_pattern: str) -> List[str]:
    """List the files matching a (optionally "file:"-prefixed) glob pattern."""
    parsed = urlparse(glob_pattern)
    if parsed.scheme == "file":
        pattern = parsed.path
    elif parsed.scheme == "":
        pattern = glob_pattern
    else:
        raise ValueError(f"Unsupported scheme {parsed.scheme!r} in {glob_pattern!r}")

    return sorted(path for path in glob.glob(pattern) if os.path.isfile(path))


def list_s3_geotiffs(s3_uri: str, key_regex: str, recursive: bool, jvm) -> List[str]:
    """List the objects under an S3 prefix whose key fully matches `key_regex`, as "s3://" URIs."""
    parsed = urlparse(s3_uri)
    bucket, prefix = parsed.netloc, parsed.path.lstrip("/")

    # reuse the S3 client of the JVM that will read the GeoTIFFs
    geotrellis = getattr(getattr(jvm.org.openeo.geotrellis, "package$"), "MODULE$")
    s3_client = geotrellis.s3Client(geotrellis.bucketRegion(bucket), None)

    request_builder = jvm.software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder()
    request_builder = request_builder.bucket(bucket).prefix(prefix)
    if not recursive:
        request_builder = request_builder.delimiter("/")

    key_pattern = re.compile(key_regex)
    s3_objects = s3_client.listObjectsV2Paginator(request_builder.build()).contents().iterator()

    return [
        f"s3://{bucket}/{key}" for key in (s3_object.key() for s3_object in s3_objects) if key_pattern.fullmatch(key)
    ]


def derive_date(path: str, date_regex: str) -> str:
    """Derive the nominal date (UTC midnight) from a path with a regex with year, month and day groups."""
    match = re.fullmatch(date_regex, path)
    if not match:
        raise ValueError(f"Date regex {date_regex!r} does not match {path!r}")

    year, month, day = match.groups()[:3]
    return dt.datetime(int(year), int(month), int(day), tzinfo=dt.timezone.utc).isoformat()


def _crs_name(crs) -> str:
    epsg_code = crs.epsgCode()
    return f"EPSG:{epsg_code.get()}" if epsg_code.isDefined() else crs.toProj4String()


def load_geotiffs(
    jvm,
    *,
    collection_id: str,
    geotiffs: Sequence[TimestampedGeoTiff],
    band_names: List[str],
    requested_band_names: List[str],
    from_date: str,
    to_date: str,
    single_level: bool,
    extent,
    extent_crs: str,
    projected_polygons,
    datacube_params,
    metadata_properties,
    correlation_id: str,
    lat_lon: bool = False,
    data_type: Optional[str] = None,
    nodata: Optional[float] = None,
):
    """
    Load timestamped GeoTIFFs as a (Scala) sequence of (zoom level, layer) tuples.

    :param band_names: the names of the bands in each GeoTIFF, in order
    :param requested_band_names: the bands to load
    :param lat_lon: use a LatLng instead of a WebMercator layout for a pyramid
    :param data_type: the cell type to convert the GeoTIFF data to (e.g. "float32")
    :param nodata: the nodata value for `data_type`
    """
    start, end = dt.datetime.fromisoformat(from_date), dt.datetime.fromisoformat(to_date)
    geotiffs = [(uri, date) for uri, date in geotiffs if start <= dt.datetime.fromisoformat(date) < end]
    logger.info(f"load_geotiffs: {len(geotiffs)} GeoTIFFs of {collection_id!r} between {from_date} and {to_date}")

    if not geotiffs:
        raise NoDataAvailableException(
            message=f"No GeoTIFFs of {collection_id!r} available for temporal extent {[from_date, to_date]}."
        )

    opensearch_client = jvm.org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient()
    first_raster = None

    for uri, date in geotiffs:
        raster_source = jvm.geotrellis.raster.geotiff.GeoTiffRasterSource(
            jvm.geotrellis.raster.geotiff.GeoTiffPath(uri), jvm.scala.Option.empty(), jvm.scala.Option.empty()
        )
        crs = _crs_name(raster_source.crs())
        raster_extent = raster_source.extent()
        native_bbox = {
            "west": raster_extent.xmin(),
            "south": raster_extent.ymin(),
            "east": raster_extent.xmax(),
            "north": raster_extent.ymax(),
            "crs": crs,
        }
        if first_raster is None:
            first_raster = (native_bbox, raster_source.cellSize())

        lonlat_bbox = reproject_bounding_box(native_bbox, from_crs=crs, to_crs="EPSG:4326")

        feature_builder = (
            jvm.org.openeo.opensearch.OpenSearchResponses.featureBuilder()
            .withId(uri)
            .withCollectionId(collection_id)
            .withNominalDate(date)
            .withCRS(crs)
            .withRasterExtent(native_bbox["west"], native_bbox["south"], native_bbox["east"], native_bbox["north"])
            .withBBox(lonlat_bbox["west"], lonlat_bbox["south"], lonlat_bbox["east"], lonlat_bbox["north"])
        )
        if data_type is not None and nodata is not None:
            feature_builder = feature_builder.addLink(uri, "data", 1.0, 0.0, band_names, data_type, float(nodata))
        elif data_type is not None:
            feature_builder = feature_builder.addLink(uri, "data", 1.0, 0.0, band_names, data_type)
        else:
            feature_builder = feature_builder.addLink(uri, "data", 1.0, 0.0, band_names)

        opensearch_client.addFeature(feature_builder.build())

    if single_level:
        layout_crs = _crs_name(projected_polygons.crs())
    else:
        layout_crs = "EPSG:4326" if lat_lon else "EPSG:3857"

    first_raster_bbox, first_raster_cell_size = first_raster
    cell_width, cell_height = reproject_cellsize(
        spatial_extent=first_raster_bbox,
        input_resolution=(float(first_raster_cell_size.width()), float(first_raster_cell_size.height())),
        input_crs=first_raster_bbox["crs"],
        to_crs=layout_crs,
    )

    pyramid_factory = jvm.org.openeo.geotrellis.file.PyramidFactory(
        opensearch_client,
        collection_id,
        requested_band_names,
        None,
        jvm.geotrellis.raster.CellSize(cell_width, cell_height),
        False,
        0.0,
    )

    if single_level:
        return pyramid_factory.datacube_seq(
            projected_polygons, from_date, to_date, metadata_properties, correlation_id, datacube_params
        )

    getattr(pyramid_factory, "crs_$eq")(jvm.geotrellis.proj4.CRS.fromName(layout_crs))
    return pyramid_factory.pyramid_seq(extent, extent_crs, from_date, to_date, metadata_properties, correlation_id)
