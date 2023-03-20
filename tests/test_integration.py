from openeo_driver.utils import read_json

from openeogeotrellis.collect_unique_process_ids_visitor import CollectUniqueProcessIdsVisitor


def test_integration(tmp_path, api100):
    process_graph = read_json("graphs/process_ids.json")["process_graph"]

    collector = CollectUniqueProcessIdsVisitor()
    collector.accept_process_graph(process_graph)
    response = api100.check_result(process_graph)

    path = tmp_path / f"response_{id(response)}.csv"
    # will be all transparent pixels, but should not give the following error:
    # 'Server error: Cannot create a polygon with exterior with fewer than 4 points: LINEARRING EMPTY'
    with path.open(mode="wb") as f:
        f.write(response.data)
