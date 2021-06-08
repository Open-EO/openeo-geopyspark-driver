import os
import warnings

# Streamline running tests when SPARK_HOME is not set.
if "SPARK_HOME" not in os.environ:
    import pyspark.find_spark_home

    spark_home = pyspark.find_spark_home._find_spark_home()
    warnings.warn("Env var SPARK_HOME was not set, setting it to {h!r}".format(h=spark_home))
    os.environ["SPARK_HOME"] = spark_home
