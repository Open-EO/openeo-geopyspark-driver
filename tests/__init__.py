import os
import warnings
from importlib.util import find_spec
from pathlib import Path
from typing import Union

from rio_cogeo import cog_validate

# Streamline running tests when SPARK_HOME is not set.
if "SPARK_HOME" not in os.environ:
    import pyspark.find_spark_home

    spark_home = pyspark.find_spark_home._find_spark_home()
    warnings.warn("Env var SPARK_HOME was not set, setting it to {h!r}".format(h=spark_home))
    os.environ["SPARK_HOME"] = spark_home

if "LD_LIBRARY_PATH" not in os.environ:
    try:
        spec = find_spec("jep")
        if spec is not None:
            os.environ["LD_LIBRARY_PATH"] = os.path.dirname(spec.origin)
    except ImportError:
        pass


def assert_cog(tiff_path: Union[str, Path]):
    is_valid_cog, errors, _ = cog_validate(str(tiff_path))
    assert is_valid_cog, str(errors)
