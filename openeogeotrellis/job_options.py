import logging
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List

from openeo_driver.constants import DEFAULT_LOG_LEVEL_PROCESSING
from openeo_driver.errors import OpenEOApiException
from openeogeotrellis.config import get_backend_config
from openeogeotrellis.constants import JOB_OPTION_LOG_LEVEL, JOB_OPTION_LOGGING_THRESHOLD
from openeogeotrellis.util.byteunit import byte_string_as

@dataclass
class JobOptions:
    """
    This class represents the available job options for openEO Geotrellis.

    """

    _log = logging.getLogger(__name__)

    driver_memory: str = field(
        default=get_backend_config().default_driver_memory,
        metadata={
            "description": "Memory allocated to the Spark ‘driver’ JVM, which manages the execution and metadata of the batch job."
        },
    )

    driver_memory_overhead: str = field(
        default=get_backend_config().default_driver_memoryOverhead,
        metadata={
            "name": "driver-memoryOverhead",
            "description": "Memory assigned to the Spark ‘driver’ on top of JVM memory for Python processes."
        })

    driver_cores: int = field(
        default=5,
        metadata={
            "description": "Number of CPUs per driver, normally not needed.",
            "public": False
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
            "description": "Memory allocated to the workers in addition to the JVM, for example, to run UDFs. "
            "The total available memory of an executor is equal to executor-memory + executor-memoryOverhead [+ python-memory].",
        })
    python_memory: str = field(
        default=get_backend_config().default_python_memory,
        metadata={
            "description": "Setting to specifically limit the memory used by python on a worker. "
            "Typical processes that use python-memory are UDF's, sar_backscatter or Sentinel 3 data loading. "
            "Leaving this setting empty will allow Python to use almost all of the executor-memoryOverhead, but may lead to unclear error messages when the memory limit is reached."
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
        },
    )  # TODO add description on how this increases requested memory

    soft_errors: str = field(
        default=get_backend_config().default_soft_errors,
        metadata={
            "description": "Ratio of soft-errors to allow in load_collection/load_stac and sar_backscatter. Reading errors can occur due to corrupted files or intermittent cloud issues."
        },
    )
    max_executors: int = field(
        default=get_backend_config().default_max_executors,
        metadata={
            "description": "The maximum number of workers assigned to the job. The maximum number of parallel tasks is max-executors*executor-cores/task-cpus. "
            "Increasing this can inflate costs, while not necessarily improving performance! "
            "Decreasing this can increase cost efficiency but will make your job slower."
        },
    )
    executor_threads_jvm: int = field(
        default=get_backend_config().default_executor_threads_jvm,
        metadata={
            "description": "Number of threads to allocate for the default jvm worker. Certain openEO processes can benefit of additional local parallelism on top of the distributed processing performed by Spark."
        },
    )
    gdal_dataset_cache_size: int = field(
        default=get_backend_config().default_gdal_dataset_cache_size,
        metadata={
            "description": "The default number of datasets that will be cached in batch jobs. A large cache will increase memory usage."
        },
    )
    udf_dependency_archives: List[str] = field(
        default_factory=list,
        metadata={"description": "An array of URLs pointing to zip files with extra dependencies to be used in UDF's."},
    )
    udf_dependency_files: List[str] = field(
        default_factory=list,
        metadata={"description": "An array of URLs pointing to files to be used in UDF's."},
    )
    openeo_jar_path: str = field(
        default=None,
        metadata={"description": "Custom jar path.", "public":False},
    )

    log_level:str = field(
        default=DEFAULT_LOG_LEVEL_PROCESSING,
        metadata={"name":"log_level","description": "log level, can be 'debug', 'info', 'warning' or 'error'", "public":True},
    )

    @staticmethod
    def as_logging_threshold_arg(value) -> str:
        value = value.upper()
        if value == "WARNING":
            value = "WARN"  # Log4j only accepts WARN whereas Python logging accepts WARN as well as WARNING

        return value

    def validate(self):


        if self.log_level not in ["DEBUG", "INFO", "WARN", "ERROR"]:
            raise OpenEOApiException(
                code="InvalidLogLevel",
                status_code=400,
                message=f"Invalid log level {self.log_level}. Should be one of 'debug', 'info', 'warning' or 'error'.",
            )

        if byte_string_as(self.executor_memory) + byte_string_as(self.executor_memory_overhead) + self.python_memory > byte_string_as(
                get_backend_config().max_executor_or_driver_memory):
            raise OpenEOApiException(
                message=f"Requested too much executor memory: {self.executor_memory} + {self.executor_memory_overhead}, the max for this instance is: {get_backend_config().max_executor_or_driver_memory}",
                status_code=400)

        if byte_string_as(self.driver_memory) + byte_string_as(self.driver_memory_overhead) > byte_string_as(
                get_backend_config().max_executor_or_driver_memory):
            raise OpenEOApiException(
                message=f"Requested too much driver memory: {self.driver_memory} + {self.driver_memory_overhead}, the max for this instance is: {get_backend_config().max_executor_or_driver_memory}",
                status_code=400)

        if (self.openeo_jar_path is not None and not self.openeo_jar_path.startswith("https://artifactory.vgt.vito.be/artifactory/libs-release-public/org/openeo/geotrellis-extensions")
                and not self.openeo_jar_path.startswith("https://artifactory.vgt.vito.be/artifactory/libs-snapshot-public/org/openeo/geotrellis-extensions/") )  :
            raise OpenEOApiException(
                message=f"Requested invalid openeo jar path {self.openeo_jar_path}",
                status_code=400)

    def soft_errors_arg(self) -> str:
        value = self.soft_errors

        if value == "false":
            return "0.0"
        elif value == None:
            return str(get_backend_config().default_soft_errors)
        elif value == "true":
            return "1.0"
        elif isinstance(value, bool):
            return "1.0" if value else "0.0"
        elif isinstance(value, (int, float)) and 0.0 <= value <= 1.0:
            return str(value)

        raise OpenEOApiException(message=f"invalid value {value} for job_option soft-errors; "
                                         f"supported values include false/true and values in the "
                                         f"interval [0.0, 1.0]",
                                 status_code=400)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobOptions":

        init_kwargs = {}
        for field in fields(cls):
            job_option_name = field.metadata.get("name", field.name.replace("_","-"))  # Use "name" from metadata or default to field name
            if job_option_name in data:

                value = data.get(job_option_name)
                cls.validate_type(value,field.type)
                init_kwargs[field.name] = value

        if JOB_OPTION_LOG_LEVEL not in init_kwargs and JOB_OPTION_LOGGING_THRESHOLD in data:
            init_kwargs[JOB_OPTION_LOG_LEVEL] = data[JOB_OPTION_LOGGING_THRESHOLD]
        if JOB_OPTION_LOG_LEVEL in init_kwargs:
            init_kwargs[JOB_OPTION_LOG_LEVEL] = JobOptions.as_logging_threshold_arg(init_kwargs[JOB_OPTION_LOG_LEVEL])


        jvmOverheadBytes = byte_string_as("128m")

        # By default, Python uses the space reserved by `spark.executor.memoryOverhead` but no limit is enforced.
        # When `spark.executor.pyspark.memory` is specified, Python will only use this memory and no more.
        python_mem = data.get("python-memory",None)
        python_max = -1
        noExplicitOverhead = "executor-memoryOverhead" not in data
        if python_mem is not None:
            python_max = byte_string_as(python_mem)

            if noExplicitOverhead:
                memOverheadBytes = jvmOverheadBytes
                init_kwargs["executor_memory_overhead"] = f"{memOverheadBytes // (1024 ** 2)}m"

        elif not noExplicitOverhead:
            # If python-memory is not set, we convert most of the overhead memory to python memory
            # this in fact duplicates the overhead memory, we should migrate away from this approach
            if cls == K8SOptions:
                memOverheadBytes = byte_string_as(data["executor-memoryOverhead"])
                python_max = memOverheadBytes - jvmOverheadBytes

        init_kwargs["python_memory"] = python_max


        return cls(**init_kwargs)


    @classmethod
    def validate_type(cls, arg, type):
        try:
            if not isinstance(arg,type):
                cls._log.warning(f"Job option with unexpected type: {arg}, expected type {cls.python_type_to_json_schema(type)}")
        except TypeError:
            pass




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

@dataclass
class K8SOptions(JobOptions):
    driver_cores: int = field(
        default=1,
        metadata={
            "description": "Number of CPUs per driver, normally not needed.",
            "public": False
        })

    executor_request_cores: int = field(
        default="NONE",
        metadata={
            "description": "Fraction of CPUs to actually request, expressed as 'milli-cpus', for instance '600m' for 0.6 of 1 cpu unit.",
            "public": False
        })
    goofys:bool = field(default=False,
        metadata={
        "description": "Deprecated, use goofys or not",
        "public": False
    })
    mount_tmp: bool = field(default=False,
         metadata={
             "description": "Deprecated, mounted tmp or not",
             "public": False
         })
    spark_pvc: bool = field(default=False,
         metadata={
             "description": "Deprecated, use spark pvcs or not",
             "public": False
         })

    def validate(self):
        max_cores = 4
        if int(self.executor_cores) > max_cores:
            raise OpenEOApiException(
                message=f"Requested too many executor cores: {self.executor_cores} , the max for this instance is: {max_cores}",
                status_code=400)
        if int(self.driver_cores) > max_cores:
            raise OpenEOApiException(
                message=f"Requested too many driver cores: {self.driver_cores} , the max for this instance is: {max_cores}",
                status_code=400)
        return super().validate()



