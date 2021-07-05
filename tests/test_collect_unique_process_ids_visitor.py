from openeo_driver.utils import read_json

from openeogeotrellis.collect_unique_process_ids_visitor import CollectUniqueProcessIdsVisitor


def test_collect_unique_process_ids():
    process_graph = read_json("tests/graphs/process_ids.json")["process_graph"]

    collector = CollectUniqueProcessIdsVisitor()
    collector.accept_process_graph(process_graph)

    assert collector.process_ids == {
        'load_collection',
        'ndvi',
        'reduce_dimension',
        'array_element',
        'lt',
        'gt',
        'or',
        'mask',
        'filter_bands',
        'save_result'
    }
