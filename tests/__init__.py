import os
import warnings
from importlib.util import find_spec

# Streamline running tests when SPARK_HOME is not set.
if "SPARK_HOME" not in os.environ:
    import pyspark.find_spark_home

    spark_home = pyspark.find_spark_home._find_spark_home()
    warnings.warn("Env var SPARK_HOME was not set, setting it to {h!r}".format(h=spark_home))
    os.environ["SPARK_HOME"] = spark_home

if "LD_LIBRARY_PATH" not in os.environ:
    try:
        os.environ["LD_LIBRARY_PATH"] = os.path.dirname(find_spec("jep").origin)
    except ImportError:
        pass