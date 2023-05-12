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


def test_handle_cli_wrapper():
    argv = ['{"process_graph":' + PG_BLOB1 + '}']
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {"id": "FOOBAR", "spatial_extent": {"west": 3, "south": 51, "east": 4, "north": 52}}
    }}


def test_handle_cli_edit_west():
    argv = ['--edit', "load.arguments.spatial_extent.west=2", PG_BLOB1]
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


def test_handle_cli_edit_multiple():
    argv = [
        '--edit', "load.arguments.spatial_extent.west=2",
        '--edit', "load.arguments.spatial_extent.east=8",
        '--edit', "load.arguments.spatial_extent.south=50",
        PG_BLOB1
    ]
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {"id": "FOOBAR", "spatial_extent": {"west": 2, "south": 50, "east": 8, "north": 52}}
    }}


def test_handle_cli_edit_inject():
    argv = ['--edit', 'load.arguments.bands=["VH", "VV"]', PG_BLOB1]
    pg, args = handle_cli(argv)
    assert pg == {"load": {
        "process_id": "load_collection",
        "arguments": {
            "id": "FOOBAR", "spatial_extent": {"west": 3, "south": 51, "east": 4, "north": 52}, "bands": ["VH", "VV"]
        }
    }}
