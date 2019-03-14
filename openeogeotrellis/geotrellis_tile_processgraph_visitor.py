from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from typing import Dict

class GeotrellisTileProcessGraphVisitor(ProcessGraphVisitor):

    def __init__(self):
        super().__init__()
        import geopyspark as gps
        jvm = gps.get_spark_context()._gateway.jvm
        self.builder = jvm.org.openeo.geotrellis.OpenEOProcessScriptBuilder()

    def enterProcess(self,process_id, arguments:Dict):
        self.builder.expressionStart(process_id,None)
        return self

    def leaveProcess(self, process_id, arguments: Dict):
        self.builder.expressionEnd(process_id, None)
        return self

    def enterArgument(self,argument_id,node:Dict):
        self.builder.argumentStart(argument_id)
        return self

    def leaveArgument(self, argument_id, node: Dict):
        self.builder.argumentEnd()
        return self

    def constantArgument(self, argument_id: str, value):
        pass

    def enterArray(self, argument_id):
        self.builder.arrayStart(argument_id)

    def constantArrayElement(self,value):
        self.builder.constantArrayElement(value)

    def arrayElementDone(self):
        self.builder.arrayElementDone()

    def leaveArray(self, argument_id):
        self.builder.arrayEnd()

