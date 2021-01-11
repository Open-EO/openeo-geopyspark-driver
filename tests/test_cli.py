import re

from openeogeotrellis.cli import handle_cli

PG_BLOB1 = re.sub(r"\s+", "", '''{
    "load": {
        "process_id": "load_collection", 
        "arguments": {
            "id": "FOOBAR", 
            "spatial_extent": {"west": 3, "south": 51, "east": 4, "north": 52}
        }}}''')


def test_handle_cli_basic():
    argv = [PG_BLOB1]
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {"id": "FOOBAR", "spatial_extent": {"west": 3, "south": 51, "east": 4, "north": 52}}
    }}


def test_handle_cli_edit_west():
    argv = [PG_BLOB1, '--edit', "load.arguments.spatial_extent.west=2"]
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {"id": "FOOBAR", "spatial_extent": {"west": 2, "south": 51, "east": 4, "north": 52}}
    }}


def test_handle_cli_edit_bbox():
    argv = [PG_BLOB1, '--edit', 'load.arguments.spatial_extent={"west":2,"east":6}']
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {"id": "FOOBAR", "spatial_extent": {"west": 2, "east": 6}}
    }}
