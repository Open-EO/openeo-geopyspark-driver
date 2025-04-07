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

    @classmethod
    def create(cls, default_input_parameter = None, default_input_datatype=None):
        builder = get_jvm().org.openeo.geotrellis.OpenEOProcessScriptBuilder()
        if default_input_datatype is not None:
            builder.setInputDataType(default_input_datatype)
        #if default_input_parameter is not None:
        #    getattr(builder, "defaultDataParameterName_$eq")(default_input_parameter)
        return cls(_builder=builder)

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
        elif isinstance(value, str):
            pass
        elif isinstance(value, bool):
            self.builder.constantArgument(argument_id, value)
        else:
            raise ValueError("Unexpected value for {a!r}: got {v!r}".format(v=value, a=argument_id))
        return self

    def enterArray(self, argument_id: str):
        self.builder.arrayStart(argument_id)

    def constantArrayElement(self, value):
        self.builder.constantArrayElement(value)

    def arrayElementDone(self, value: dict):
        self.builder.arrayElementDone()

    def leaveArray(self, argument_id: str):
        self.builder.arrayEnd()

    def _accept_dict(self, value: dict):
        if 'process_graph' in value:
            self.accept_process_graph(value['process_graph'])


class SingleNodeUDFProcessGraphVisitor(ProcessGraphVisitor):
    def __init__(self):
        super().__init__()
        self.udf_args = {}

    def enterArgument(self, argument_id: str, value):
        self.udf_args[argument_id] = value

    def constantArgument(self, argument_id: str, value):
        self.udf_args[argument_id] = value


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


if __name__ == "__main__":
    if sys.argv[1:]:
        (process_graph,) = sys.argv[1:]
        if process_graph.strip().startswith("{"):
            process_graph = json.loads(process_graph)
        elif process_graph.strip().endswith(".json"):
            with open(process_graph) as f:
                process_graph = json.load(f)
        elif process_graph.strip() == "-":
            process_graph = json.load(sys.stdin)
        else:
            raise ValueError(process_graph)
    else:
        # Default example
        process_graph = {
    "arrayapply1": {
      "arguments": {
        "data": {
          "from_parameter": "data"
        },
        "process": {
          "process_graph": {
            "absolute1": {
              "arguments": {
                "x": {
                  "from_node": "multiply1"
                }
              },
              "process_id": "absolute"
            },
            "absolute2": {
              "arguments": {
                "x": {
                  "from_node": "datedifference1"
                }
              },
              "process_id": "absolute"
            },
            "add1": {
              "arguments": {
                "x": {
                  "from_node": "absolute1"
                },
                "y": {
                  "from_node": "absolute2"
                }
              },
              "process_id": "add",
              "result": True
            },
            "datedifference1": {
              "arguments": {
                "date1": {
                  "from_node": "datereplacecomponent1"
                },
                "date2": {
                  "from_parameter": "label"
                },
                "unit": "day"
              },
              "process_id": "date_difference"
            },
            "datereplacecomponent1": {
              "arguments": {
                "component": "day",
                "date": {
                  "from_parameter": "label"
                },
                "value": 15
              },
              "process_id": "date_replace_component"
            },
            "multiply1": {
              "arguments": {
                "x": 15,
                "y": {
                  "from_parameter": "x"
                }
              },
              "process_id": "multiply"
            }
          }
        }
      },
      "process_id": "array_apply"
    },
    "arrayapply3": {
      "arguments": {
        "data": {
          "from_node": "arrayapply1"
        },
        "process": {
          "process_graph": {
            "arrayapply2": {
              "arguments": {
                "data": {
                  "from_parameter": "data"
                },
                "process": {
                  "process_graph": {
                    "absolute3": {
                      "arguments": {
                        "x": {
                          "from_node": "multiply2"
                        }
                      },
                      "process_id": "absolute"
                    },
                    "absolute4": {
                      "arguments": {
                        "x": {
                          "from_node": "datedifference2"
                        }
                      },
                      "process_id": "absolute"
                    },
                    "add2": {
                      "arguments": {
                        "x": {
                          "from_node": "absolute3"
                        },
                        "y": {
                          "from_node": "absolute4"
                        }
                      },
                      "process_id": "add",
                      "result": True
                    },
                    "datedifference2": {
                      "arguments": {
                        "date1": {
                          "from_node": "datereplacecomponent2"
                        },
                        "date2": {
                          "from_parameter": "label"
                        },
                        "unit": "day"
                      },
                      "process_id": "date_difference"
                    },
                    "datereplacecomponent2": {
                      "arguments": {
                        "component": "day",
                        "date": {
                          "from_parameter": "label"
                        },
                        "value": 15
                      },
                      "process_id": "date_replace_component"
                    },
                    "multiply2": {
                      "arguments": {
                        "x": 15,
                        "y": {
                          "from_parameter": "x"
                        }
                      },
                      "process_id": "multiply"
                    }
                  }
                }
              },
              "process_id": "array_apply"
            },
            "int1": {
              "arguments": {
                "x": {
                  "from_parameter": "x"
                }
              },
              "process_id": "int"
            },
            "int2": {
              "arguments": {
                "x": {
                  "from_node": "min1"
                }
              },
              "process_id": "int"
            },
            "min1": {
              "arguments": {
                "data": {
                  "from_node": "arrayapply2"
                }
              },
              "process_id": "min"
            },
            "neq1": {
              "arguments": {
                "x": {
                  "from_node": "int1"
                },
                "y": {
                  "from_node": "int2"
                }
              },
              "process_id": "neq",
              "result": True
            }
          }
        }
      },
      "process_id": "array_apply",
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
