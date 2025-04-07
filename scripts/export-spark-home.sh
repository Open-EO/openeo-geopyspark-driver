# Sourcing snippet to set SPARK_HOME env variable to make geopyspark happy.
# Otherwise `import geopyspark` complains like this:
#
#         import geopyspark
#      File ".../site-packages/geopyspark/__init__.py", line 5, in <module>
#        ensure_pyspark()
#      File ".../site-packages/geopyspark/geopyspark_utils.py", line 11, in ensure_pyspark
#        add_pyspark_path()
#      File ".../site-packages/geopyspark/geopyspark_utils.py", line 28, in add_pyspark_path
#        raise KeyError("Could not find SPARK_HOME")
#    KeyError: 'Could not find SPARK_HOME'

echo "Currently: SPARK_HOME=$SPARK_HOME"

if [[ "$(basename -- "$0")" == "export-spark-home.sh" ]]; then
  echo "ERROR:" >&2
  echo "This script is intended to be sourced, like:" >&2
  echo "    source $0" >&2
  echo "Running it as a script will not set the environment variable in your shell" >&2
else
  # This assumes working in a virtual env with pyspark installed
  # (`find_spark_home.py` is provided by pyspark)
  spark_home=$(find_spark_home.py)
  if [[ -n "$spark_home" ]]; then
    export SPARK_HOME=$spark_home
    echo "exported SPARK_HOME=$SPARK_HOME"
  else
    echo "Failed to find Spark home" >&2
  fi
fi
