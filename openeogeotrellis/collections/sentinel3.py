import json
import logging
import os
import pathlib
import sys
from glob import glob
from typing import Tuple

import pyspark
from pyspark import SparkContext, find_spark_home

from openeogeotrellis.utils import get_jvm

OLCI_PRODUCT_TYPE = "OL_1_EFR___"


def main(bbox, crs, from_date, to_date):
    spark_python = os.path.join(find_spark_home._find_spark_home(), 'python')
    py4j = glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
    sys.path[:0] = [spark_python, py4j]

    import geopyspark as gps
    from openeogeotrellis.collections.s1backscatter_orfeo import S1BackscatterOrfeoV2

    conf = gps.geopyspark_conf(master="local[*]", appName=__file__)
    conf.set('spark.kryo.registrator', 'geotrellis.spark.store.kryo.KryoRegistrator')
    conf.set('spark.driver.extraJavaOptions', "-Dlog4j2.debug=false -Dlog4j2.configurationFile=file:/home/bossie/PycharmProjects/openeo/openeo-python-driver/log4j2.xml")

    with SparkContext(conf=conf):
        jvm = get_jvm()

        opensearch_client = jvm.org.openeo.opensearch.OpenSearchClient.apply(
            "https://catalogue.dataspace.copernicus.eu/resto", False, "", [], "",
        )

        collection_id = "Sentinel3"
        attribute_values = {"productType": OLCI_PRODUCT_TYPE}
        correlation_id = ""

        file_rdd_factory = jvm.org.openeo.geotrellis.file.FileRDDFactory(
            opensearch_client, collection_id, [], attribute_values, correlation_id,
        )

        extent = jvm.geotrellis.vector.Extent(*bbox)
        projected_polygons = jvm.org.openeo.geotrellis.ProjectedPolygons.fromExtent(extent, crs)
        # do in EPSG:4326 (~ SENTINEL3_OLCI_L1B on Terrascope)
        #  ProjectedPolygons.crs() is "EPSG:4326"
        #  ProjectedPolygons.polygons() is in "EPSG:4326"
        #  maxSpatialResolution is CellSize(0.00297619047619,0.00297619047619)
        #  result of DatacubeSupport.layerMetadata is: TileLayerMetadata(float32ud0.0,LayoutDefinition(Extent(5.027, 50.45939523809536, 5.78890476190464, 51.2213),CellSize(0.0029761904761900007,0.0029761904761899938),1x1 tiles,256x256 pixels),Extent(5.027, 51.1974, 5.0438, 51.2213),EPSG:4326,KeyBounds(SpaceTimeKey(0,0,1533513600000),SpaceTimeKey(0,0,1533599999999)))
        target_epsg_code = 4326
        projected_polygons_native_crs = (getattr(getattr(jvm.org.openeo.geotrellis, "ProjectedPolygons$"), "MODULE$")
                                         .reproject(projected_polygons, target_epsg_code))

        zoom = 0
        tile_size = 256

        data_cube_parameters = jvm.org.openeo.geotrelliscommon.DataCubeParameters()
        getattr(data_cube_parameters, "tileSize_$eq")(tile_size)
        getattr(data_cube_parameters, "layoutScheme_$eq")("FloatingLayoutScheme")
        data_cube_parameters.setGlobalExtent(*bbox, crs)

        keyed_feature_rdd = file_rdd_factory.loadSpatialFeatureJsonRDD(
            projected_polygons_native_crs, from_date, to_date, zoom, tile_size, data_cube_parameters
        )

        jrdd = keyed_feature_rdd._1()
        metadata_sc = keyed_feature_rdd._2()

        j2p_rdd = jvm.SerDe.javaToPython(jrdd)
        serializer = pyspark.serializers.PickleSerializer()
        pyrdd = gps.create_python_rdd(j2p_rdd, serializer=serializer)
        pyrdd = pyrdd.map(json.loads)

        metadata = S1BackscatterOrfeoV2(jvm)._convert_scala_metadata(metadata_sc)

        # -----------------------------------
        prefix = ""

        def process_feature(feature: dict) -> Tuple[str, dict]:
            creo_path = prefix + feature["feature"]["id"]
            return creo_path, {
                "key": feature["key"],
                "key_extent": feature["key_extent"],
                "bbox": feature["feature"]["bbox"],
                "key_epsg": feature["metadata"]["crs_epsg"]
            }

        per_product = pyrdd.map(process_feature).groupByKey().mapValues(list)

        olci_layer = per_product.flatMap(read_olci)

        print(olci_layer.count())

        # TODO: create a TileLayerRDD (GeoPySpark equivalent)
        # TODO: stitch it and write to file


def read_olci(product):
    from openeogeotrellis.collections.s1backscatter_orfeo import get_total_extent

    creo_path, features = product  # better: "tiles"
    log_prefix = ""

    creo_path = pathlib.Path(creo_path)
    if not creo_path.exists():
        raise Exception(f"read_olci: path {creo_path} does not exist.")

    # Get whole extent of tile layout
    col_min = min(f["key"]["col"] for f in features)
    col_max = max(f["key"]["col"] for f in features)
    cols = col_max - col_min + 1
    row_min = min(f["key"]["row"] for f in features)
    row_max = max(f["key"]["row"] for f in features)
    rows = row_max - row_min + 1
    instants = set(f["key"]["instant"] for f in features)
    assert len(instants) == 1, f"Not single instant: {instants}"
    instant = instants.pop()
    print(
        f"{log_prefix} Layout key extent: col[{col_min}:{col_max}] row[{row_min}:{row_max}]"
        f" ({cols}x{rows}={cols * rows} tiles) instant[{instant}]."
    )

    layout_extent = get_total_extent(features)

    key_epsgs = set(f["key_epsg"] for f in features)
    assert len(key_epsgs) == 1, f"Multiple key CRSs {key_epsgs}"
    layout_epsg = key_epsgs.pop()

    print(
        f"{log_prefix} Layout extent {layout_extent} EPSG {layout_epsg}:"
    )

    create_s3_toa(creo_path,
                  [layout_extent['xmin'], layout_extent['ymin'], layout_extent['xmax'], layout_extent['ymax']])

    # TODO: cut the grid up in tiles again

    return features


def create_s3_toa(creo_path, bbox_tile):
    # TODO: do the TOA-S3 thing on this envelope into an xarray
    return


if __name__ == '__main__':
    logging.basicConfig(level="INFO")

    bbox = [2.535352308127358, 50.57415247573394, 5.713651867060349, 51.718230797191836]
    crs = "EPSG:4326"
    from_date = "2017-03-06T00:00:00Z"
    to_date = "2017-03-06T10:39:01Z"

    main(bbox, crs, from_date, to_date)
