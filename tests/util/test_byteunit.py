import pytest

import geopyspark as gps

from openeogeotrellis.util.byteunit import byte_string_as


def test_bytestring_as_gb():
    jvm = gps.get_spark_context()._jvm
    as_bytes = jvm.org.apache.spark.util.Utils.byteStringAsBytes

    assert byte_string_as("1500m") == as_bytes("1500m")
    assert byte_string_as("2g") == as_bytes("2g")


def test_bytestring_fraction():
    with pytest.raises(ValueError):
        byte_string_as("1.5g")
