import numbers
from collections import OrderedDict

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


class GeotrellisTileProcessGraphVisitor(ProcessGraphVisitor):

    def __init__(self):
        super().__init__()
        import geopyspark as gps
        jvm = gps.get_spark_context()._gateway.jvm
        self.builder = jvm.org.openeo.geotrellis.OpenEOProcessScriptBuilder()
        #process list to keep track of processes, so this class has a double function
        self.processes = OrderedDict()

    def enterProcess(self, process_id: str, arguments: dict):
        self.builder.expressionStart(process_id, arguments)
        self.processes[process_id] = arguments
        return self

    def leaveProcess(self, process_id: str, arguments: dict):
        self.builder.expressionEnd(process_id, arguments)
        return self

    def enterArgument(self, argument_id: str, value):
        self.builder.argumentStart(argument_id)
        return self

    def leaveArgument(self, argument_id: str, value):
        self.builder.argumentEnd()
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
