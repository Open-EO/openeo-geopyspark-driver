import gzip
import json
import zipfile
from pathlib import Path

import pytest

from openeogeotrellis.catalog.files import load_catalog_files, read_catalog_file
from tests.data import get_test_data_file


@pytest.fixture
def layercatalog_json_gz(tmp_path) -> Path:
    """Fixture for building a layercatalog.json.gz file"""
    layer_catalog_data = [
        {"id": "ZOO", "description": "The ZOO layer"},
        {"id": "ZAZ", "description": "The ZAZ layer"},
    ]
    path = tmp_path / "layercatalog01.json.gz"
    assert not path.exists()

    with gzip.open(path, mode="wt") as f:
        json.dump(obj=layer_catalog_data, fp=f, indent=2)
    # Check GZIP signature
    assert path.read_bytes()[:2] == b"\x1f\x8b"

    return path


def test_read_catalog_file_json():
    path = get_test_data_file("layercatalog01.json")
    assert read_catalog_file(path) == {
        "BAR": {"id": "BAR", "description": "bar", "links": ["example.com/bar"]},
        "BZZ": {"id": "BZZ"},
        "FOO": {"id": "FOO", "license": "mit"},
    }


def test_read_catalog_file_json_gz(layercatalog_json_gz):
    assert read_catalog_file(layercatalog_json_gz) == {
        "ZAZ": {"id": "ZAZ", "description": "The ZAZ layer"},
        "ZOO": {"id": "ZOO", "description": "The ZOO layer"},
    }


def test_read_catalog_file_zip(tmp_path):
    layercatalog_zip = tmp_path / "layercatalog.zip"
    with zipfile.ZipFile(layercatalog_zip, mode="w") as zf:
        # A file with single, top-level collection (no list)
        zf.writestr(
            "one-collection.json",
            json.dumps({"id": "THE_ONE"}),
        )
        # A file with list of collections
        zf.writestr(
            "multiple.json",
            json.dumps([{"id": "TWO"}, {"id": "THREE"}]),
        )
    # Check ZIP signature
    assert layercatalog_zip.read_bytes()[:2] == b"\x50\x4b"

    assert read_catalog_file(layercatalog_zip) == {
        "THE_ONE": {"id": "THE_ONE"},
        "TWO": {"id": "TWO"},
        "THREE": {"id": "THREE"},
    }


def test_load_catalog_files_from_gzip(layercatalog_json_gz):
    metadata = load_catalog_files(catalog_files=[str(layercatalog_json_gz)], enrich_metadata=False)
    assert sorted(metadata.keys()) == ["ZAZ", "ZOO"]


def test_load_catalog_files_merges_multiple_files(tmp_path):
    path1 = get_test_data_file("layercatalog01.json")
    path2 = tmp_path / "layercatalog02.json"
    path2.write_text(json.dumps([{"id": "EXTRA"}]))
    metadata = load_catalog_files(catalog_files=[str(path1), str(path2)], enrich_metadata=False)
    assert sorted(metadata.keys()) == ["BAR", "BZZ", "EXTRA", "FOO"]
