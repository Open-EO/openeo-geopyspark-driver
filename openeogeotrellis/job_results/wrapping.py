"""
Wraps the raw process-graph evaluation result into a list of `SaveResult`s.
"""
from typing import Any, List

from openeo_driver.datacube import DriverDataCube, DriverVectorCube
from openeo_driver.delayed_vector import DelayedVector
from openeo_driver.save_result import ImageCollectionResult, JSONResult, SaveResult, VectorCubeResult
from shapely.geometry import mapping


def wrap_evaluation_result(result: Any, *, job_specification: dict) -> List[SaveResult]:
    if isinstance(result, DelayedVector):
        geojsons = (mapping(geometry) for geometry in result.geometries_wgs84)
        result = JSONResult(geojsons)

    if isinstance(result, DriverDataCube):
        format_options = job_specification.get("output", {})
        format_options["batch_mode"] = True
        result = ImageCollectionResult(cube=result, format="GTiff", options=format_options)

    if isinstance(result, DriverVectorCube):
        format_options = job_specification.get("output", {})
        format_options["batch_mode"] = True
        result = VectorCubeResult(cube=result, format="GTiff", options=format_options)

    results = result if isinstance(result, List) else [result]
    results = [result if isinstance(result, SaveResult) else JSONResult(result) for result in results]
    return results
