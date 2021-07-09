import os
import sys
import time
from pyspark import SparkContext, SparkConf
import openeo
from openeo import ImageCollection
from openeo_driver.utils import EvalEnv
from openeogeotrellis.backend import GeoPySparkBackendImplementation
from openeogeotrellis.utils import kerberos
from openeo_driver import ProcessGraphDeserializer
# from openeo_driver.utils import replace_nan_values
from functools import reduce
from typing import Callable, Dict, List
# import json
import geopandas as gpd

"""
To run locally, unset SPARK_HOME, then:

SPARK_HOME=$(find_spark_home.py) HADOOP_CONF_DIR=/etc/hadoop/conf spark-submit \
--master "local[*]" \
--principal vdboschj@VGT.VITO.BE --keytab ${HOME}/Documents/VITO/vdboschj.keytab \
--jars jars/geotrellis-backend-assembly-0.4.7-openeo.jar,jars/geotrellis-extensions-2.2.0-SNAPSHOT.jar \
benchmarks/benchmark.py

To debug application in PyCharm, set SPARK_HOME, HADOOP_CONF_DIR, then use e.g.
_local_spark_conf()
"""


def _time(action: Callable[[], object]) -> (object, float):
    start = time.time()
    result = action()
    end = time.time()

    return result, end - start


def _huge_vector_file_time_series(vector_file, start_date, end_date) -> ImageCollection:
    min_x, min_y, max_x, max_y = gpd.read_file(vector_file).total_bounds

    session = openeo.connect("http://openeo.vgt.vito.be/openeo/0.4.0")

    request = session \
        .imagecollection('S1_GRD_SIGMA0_ASCENDING') \
        .filter_temporal(start_date, end_date) \
        .filter_bbox(west=min_x, east=max_x, north=max_y, south=min_y, crs='EPSG:4326') \
        .polygonal_mean_timeseries(vector_file)

    # FIXME: do graph_add_process('save_result') ?
    request.graph[request.node_id]['result'] = 'true'

    return request


def _local_spark_conf() -> SparkConf:
    import geopyspark as gps
    conf = gps.geopyspark_conf(master="local[*]", appName="benchmark.py")
    conf.set('spark.yarn.keytab', "/home/bossie/Documents/VITO/vdboschj.keytab")
    conf.set('spark.yarn.principal', "vdboschj")

    return conf


def main(argv: List[str]) -> None:
    iterations = int(argv[1]) if len(argv) > 1 else 1

    start_date = argv[2] if len(argv) > 2 else '2018-01-01'
    end_date = argv[3] if len(argv) > 3 else start_date

    print("%d iteration(s) from %s to %s" % (iterations, start_date, end_date))

    vector_files = [
        #  ("1", "/data/users/Public/vdboschj/EP-3025/GeometryCollection.shp"),
        #  ("63 overlapping", "/data/users/Public/vdboschj/EP-3025/BELCAM_fields_2017_winter_wheat_4326.shp"),
        #  ("61 non-overlapping", "/data/users/Public/vdboschj/EP-3025/BELCAM_fields_2017_winter_wheat_4326_non_overlapping.shp"),
        #  ("25 K overlapping", "/data/users/Public/driesj/fields_flanders_non_overlap.shp"),
        #  ("18 K non-overlapping", "/data/users/Public/driesj/fields_flanders_zero_overlap.shp")
        ("59 K overlapping", "/data/users/Public/driesj/fields_flanders_zero_overlap_59350.shp"),
        ("59 K non-overlapping", "/data/users/Public/driesj/fields_flanders_zero_overlap_59338.shp")
    ]

    sc = SparkContext.getOrCreate(conf=None)

    env = EvalEnv({
        "version": "1.0.0",
        "pyramid_levels": "highest",
        "correlation_id": f"benchmark-pid{os.getpid()}",
        "backend_implementation": GeoPySparkBackendImplementation(),
    })

    try:
        for context, vector_file in vector_files:
            process_graph = _huge_vector_file_time_series(vector_file, start_date, end_date).graph

            def evaluate() -> Dict:
                principal = sc.getConf().get("spark.yarn.principal")
                key_tab = sc.getConf().get("spark.yarn.keytab")

                kerberos(principal, key_tab)
                return ProcessGraphDeserializer.evaluate(process_graph, env=env)

            def combine_iterations(acc: (Dict, float), i: int) -> (Dict, float):
                count = i + 1
                _, duration_sum = acc

                res, duration = _time(evaluate)
                print("iteration %d of %d: evaluation of %s (%s) took %f seconds" % (count, iterations, vector_file,
                                                                                     context, duration))

                return res, duration_sum + duration

            (result, total_duration) = reduce(combine_iterations, range(iterations), (None, 0))

            # json_result = json.dumps(replace_nan_values(result), sort_keys=True, indent=2, separators=(',', ': '))

            print("evaluation of %s (%s) took %f seconds on average" % (vector_file, context, total_duration / iterations))
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
