from dataclasses import dataclass, field, fields
from typing import Any, Dict, List

from openeogeotrellis.config import get_backend_config


@dataclass
class JobOptions:
    """
    This class represents the available job options for openEO Geotrellis.

    """

    driver_memory: str = field(default=get_backend_config().default_driver_memory , metadata={
        "description": "Memory allocated to the Spark ‘driver’ JVM, which manages the execution of the batch job."
    })

    driver_memory_overhead: str = field(
        default=get_backend_config().default_driver_memoryOverhead,
        metadata={
            "name": "driver-memoryOverhead",
            "description": "Memory assigned to the Spark ‘driver’ on top of JVM memory for Python processes."
        })

    executor_memory: str = field(
        default=get_backend_config().default_executor_memory,
        metadata={
            "description": "Memory allocated to the workers for the JVM that executes most predefined processes."
        })
    executor_memory_overhead: str = field(
        default=get_backend_config().default_executor_memoryOverhead,
        metadata={
            "name": "executor-memoryOverhead",
            "description": "Allocated in addition to the JVM, for example, to run UDFs."
        })
    python_memory: str = field(
        default=get_backend_config().default_python_memory,
        metadata={
            "description": "Memory assigned to Python UDFs, sar_backscatter or Sentinel-3 data loading tasks."
        })
    executor_cores: int = field(
        default=get_backend_config().default_executor_cores,
        metadata={
            "description": "Number of CPUs per worker (executor). The number of parallel tasks is calculated as executor-cores / task-cpus. Setting this value equal to task-cpus is recommended to avoid potential GDAL reading errors."
        })
    task_cpus: int = field(
        default=1,
        metadata={
            "description": "CPUs assigned to a single task. UDFs using libraries like Tensorflow can benefit from further parallelization at the individual task level."
        })
    executor_corerequest: str = field(
        default="",
        metadata={
            "description": "This setting is only relevant for Kubernetes-based backends, allows overcommitting CPU."})
    max_executors: int = field(
        default=get_backend_config().default_max_executors,
        metadata={

        "description": "The maximum number of workers assigned to the job. The maximum number of parallel tasks is max-executors*executor-cores/task-cpus. Increasing this can inflate costs, while not necessarily improving performance!"})
    udf_dependency_archives: List[str] = field(
        default = None,
        metadata={"description": "An array of URLs pointing to zip files with extra dependencies; see below."})

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobOptions":

        init_kwargs = {}
        for field in fields(cls):
            field_name = field.metadata.get("name", field.name.replace("_","-"))  # Use "name" from metadata or default to field name
            init_kwargs[field.name] = data.get(field_name, field.default)

        return cls(**init_kwargs)


    @classmethod
    def list_options(cls, public_only=True):
        """
        List all available options and their descriptions, following the schema defined by openEO parameters extension
        https://github.com/Open-EO/openeo-api/blob/draft/extensions/processing-parameters/openapi.yaml
        """
        options = []
        for field in fields(cls):
            if public_only and field.metadata.get("public", False):
                continue
            options.append({
                "name": field.metadata.get("name", field.name.replace("_","-")),
                "description": field.metadata.get("description", ""),
                "optional": True,
                "default": field.default,
                "schema": cls.python_type_to_json_schema(field.type)
            })
        return options

    @classmethod
    def python_type_to_json_schema(cls, python_type):
        """
        Converts a Python type or instance to a basic JSON schema representation.

        Args:
            python_type: A Python type (e.g., str, int, list, dict) or an instance of a type.

        Returns:
            A dictionary representing the basic JSON schema for the given Python type.
        """
        if isinstance(python_type, type):
            if python_type is str:
                return {"type": "string"}
            elif python_type is int:
                return {"type": "integer"}
            elif python_type is float:
                return {"type": "number"}
            elif python_type is bool:
                return {"type": "boolean"}
            elif python_type is list:
                return {"type": "array", "items": {}}  # Need more info for item type
            elif python_type is dict:
                return {"type": "object", "properties": {}}  # Need more info for properties
            elif python_type is type(None):
                return {"type": "null"}
            else:
                raise ValueError(f"JSON schema convert does not support {type}")
        elif List[str] == python_type:
            return {"type": "array", "items": {"type": "string"}}
        else:
            # Handle instances
            return cls.python_type_to_json_schema(type(python_type))
