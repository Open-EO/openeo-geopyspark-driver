import json
import numbers
import sys
from collections import OrderedDict
from typing import Union

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeogeotrellis.utils import get_jvm


class GeotrellisTileProcessGraphVisitor(ProcessGraphVisitor):

    def __init__(self, _builder=None):
        super().__init__()
        self.builder = _builder or get_jvm().org.openeo.geotrellis.OpenEOProcessScriptBuilder()
        # process list to keep track of processes, so this class has a double function
        self.processes = OrderedDict()

    def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        self.builder.expressionStart(process_id, arguments)
        # TODO: store/use namespace?
        self.processes[process_id] = arguments
        return self

    def leaveProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
        # TODO: store/use namespace?
        self.builder.expressionEnd(process_id, arguments)
        return self

    def enterArgument(self, argument_id: str, value):
        self.builder.argumentStart(argument_id)
        return self

    def leaveArgument(self, argument_id: str, value):
        self.builder.argumentEnd()
        return self

    def from_parameter(self, parameter_id: str):
        self.builder.fromParameter(parameter_id)
        return self

    def constantArgument(self, argument_id: str, value):
        if isinstance(value, numbers.Real):
            self.builder.constantArgument(argument_id, value)
        else:
            raise ValueError("Expecting numeric value for {a!r} but got {v!r}".format(v=value, a=argument_id))
        return self

    def enterArray(self, argument_id: str):
        self.builder.arrayStart(argument_id)

    def constantArrayElement(self, value):
        self.builder.constantArrayElement(value)

    def arrayElementDone(self, value: dict):
        self.builder.arrayElementDone()

    def leaveArray(self, argument_id: str):
        self.builder.arrayEnd()


class FakeGeotrellisTileProcessGraphVisitor(GeotrellisTileProcessGraphVisitor):
    """
    Fake GeotrellisTileProcessGraphVisitor that just prints out all
    the OpenEOProcessScriptBuilder calls it would do (instead of executing them).
    Helps with building the correct builder call sequence for
    OpenEOProcessScriptBuilder unit tests in openeo-geotrellis-extension.
    """

    class _FakeBuilder:
        def __getattr__(self, item):
            def print_call(*args):
                # Use `json` instead of `repr` to get double quoted strings (compatible with C/Java/Scala).
                call_args = ", ".join(json.dumps(a) for a in args)
                print(f"builder.{item}({call_args});")

            return print_call

    def __init__(self):
        super().__init__(_builder=self._FakeBuilder())


if __name__ == '__main__':
    if sys.argv[1:]:
        process_graph, = sys.argv[1:]
        if process_graph.strip().startswith('{'):
            process_graph = json.loads(process_graph)
        elif process_graph.strip().endswith('.json'):
            with open(process_graph) as f:
                process_graph = json.load(f)
        elif process_graph.strip() == "-":
            process_graph = json.load(sys.stdin)
        else:
            raise ValueError(process_graph)
    else:
        # Default example
        process_graph = {
            "band0": {
                "process_id": "array_element",
                "arguments": {"data": {"from_parameter": "data"}, "index": 0}
            },
            "band1": {
                "process_id": "array_element",
                "arguments": {"data": {"from_parameter": "data"}, "index": 1}
            },
            "add": {
                "process_id": "add",
                "arguments": {"x": {"from_node": "band0"}, "y": {"from_node": "band1"}},
                "result": True
            }
        }

    visitor = FakeGeotrellisTileProcessGraphVisitor()
    visitor.accept_process_graph(process_graph)
    # This prints out something like:
    #     expressionStart('add', {'x': {'from_node': 'band0', 'node': {'process_id': ...
    #     argumentStart('x')
    #     expressionStart('array_element', {'data': {'from_parameter': 'data'}, 'index': 0})
    #     argumentStart('data')
    #     fromParameter('data')
    #     ....
