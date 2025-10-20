from pathlib import Path

import boto3
import moto
import pystac
import pytest
import responses

from openeo.testing.stac import StacDummyBuilder
from openeogeotrellis.integrations.stac import (
    ResilientStacIO,
    S3StacIO,
    CompositeStacIO,
    ComposableStacIO,
    ref_as_str,
)


class TestResilientStacIO:

    @responses.activate
    def test_basic(self):
        responses.get("https://example.test", json={"hello": "world"})

        stac_api_io = ResilientStacIO()
        assert stac_api_io.read_text_from_href("https://example.test") == '{"hello": "world"}'

    @responses.activate
    def test_http_error(self):
        responses.get("https://example.test", status=500)

        stac_api_io = ResilientStacIO()
        with pytest.raises(Exception, match="Could not read uri https://example.test"):
            stac_api_io.read_text_from_href("https://example.test")


def test_ref_as_str_simple_str():
    assert ref_as_str("https://stac.test/item.json") == "https://stac.test/item.json"
    assert ref_as_str("s3://bucket/key") == "s3://bucket/key"


def test_ref_as_str_link():
    link = pystac.Link(rel="self", target="s3://bucket/key")
    assert ref_as_str(link) == "s3://bucket/key"


def test_ref_as_str_link_with_owning_collection():
    link = pystac.Link(rel="item", target="item.json")
    collection = pystac.Collection.from_dict(
        StacDummyBuilder.collection(links=[{"rel": "self", "href": "s3://bucket/collection123"}])
    )
    link.set_owner(collection)
    assert ref_as_str(link) == "s3://bucket/item.json"


class _ReverseStacIO(ComposableStacIO):
    """Dummy STAC IO that reverses text content for files ending with .nosj"""

    def supports(self, ref):
        return ref_as_str(ref).endswith(".nosj")

    def read_text(self, source, *args, **kwargs):
        return Path(source).read_text()[::-1]

    def write_text(self, dest, txt, *args, **kwargs):
        txt = txt[::-1]
        return Path(dest).write_text(txt)


class TestCompositeStacIO:
    def test_read_text(self, tmp_path):
        item1_path = tmp_path / "item.json"
        item1_path.write_text("Hello world!")
        item2_path = tmp_path / "item.nosj"
        item2_path.write_text("!dlrow olleH")

        stac_io = CompositeStacIO(stac_ios=[_ReverseStacIO()])

        assert stac_io.read_text(str(item1_path)) == "Hello world!"
        assert stac_io.read_text(str(item2_path)) == "Hello world!"

    def test_write_text(self, tmp_path):
        item1_path = tmp_path / "item.json"
        item2_path = tmp_path / "item.nosj"

        stac_io = CompositeStacIO(stac_ios=[_ReverseStacIO()])

        stac_io.write_text(str(item1_path), "Hello world!")
        stac_io.write_text(str(item2_path), "Hello world!")

        assert item1_path.read_text() == "Hello world!"
        assert item2_path.read_text() == "!dlrow olleH"


class TestS3StacIO:
    @pytest.fixture(autouse=True)
    def s3(self, monkeypatch, moto_server):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")

        bucket = "thebucket"
        s3_client = boto3.client("s3", endpoint_url=moto_server)
        s3_client.create_bucket(Bucket=bucket)

        return s3_client

    @pytest.mark.parametrize(
        "ref_factory",
        [
            str,
            lambda s: pystac.Link(rel="self", target=s),
        ],
    )
    def test_read_text(self, s3, ref_factory):
        s3.put_object(Bucket="thebucket", Key="path/to/item.json", Body='{"type": "Feature", "id": "item123"}')

        stac_io = S3StacIO()
        source = ref_factory("s3://thebucket/path/to/item.json")
        assert stac_io.read_text(source) == '{"type": "Feature", "id": "item123"}'

    @pytest.mark.parametrize(
        "ref_factory",
        [
            str,
            lambda s: pystac.Link(rel="self", target=s),
        ],
    )
    def test_write_text(self, s3, ref_factory):
        stac_io = S3StacIO()
        dest = ref_factory("s3://thebucket/path/to/item.json")
        stac_io.write_text(dest, txt='{"type": "Feature", "id": "item123"}')

        response = s3.get_object(Bucket="thebucket", Key="path/to/item.json")
        body = response["Body"].read().decode("utf-8")
        assert body == '{"type": "Feature", "id": "item123"}'
