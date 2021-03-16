from typing import Union

from openeo.internal.process_graph_visitor import ProcessGraphVisitor


def extract_literal_match(condition) -> (str, object):
    """
    Turns a condition as defined by the load_collection process into a (key, value) pair; therefore, conditions are
    currently limited to exact matches ("eq").
    """
    class LiteralMatchExtractingGraphVisitor(ProcessGraphVisitor):
        def __init__(self):
            super().__init__()
            self.property_value = None

        def enterProcess(self, process_id: str, arguments: dict, namespace: Union[str, None]):
            if process_id != 'eq':
                raise NotImplementedError("process %s is not supported" % process_id)

        def enterArgument(self, argument_id: str, value):
            assert value['from_parameter'] == 'value'

        def constantArgument(self, argument_id: str, value):
            if argument_id in ['x', 'y']:
                self.property_value = value

    if isinstance(condition, dict) and 'process_graph' in condition:
        predicate = condition['process_graph']
        property_value = LiteralMatchExtractingGraphVisitor().accept_process_graph(
            predicate).property_value
        return property_value
    else:
        return condition
