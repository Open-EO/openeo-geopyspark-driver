from typing import Union

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class CollectUniqueProcessIdsVisitor(ProcessGraphVisitor):
    def __init__(self):
        super().__init__()
        self.process_ids = set()

    def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        self.process_ids.add(process_id)

    def _accept_dict(self, value: dict):
        if 'process_graph' in value:
            self.accept_process_graph(value['process_graph'])
