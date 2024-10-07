#!/usr/bin/env python3

import os
from pathlib import Path

import openeo

# This example makes a small process graph and executes it in a local OpenEO container.

#############################
# Step 1, build process graph
#############################
spatial_extent_tap = {"east": 5.08, "north": 51.22, "south": 51.215, "west": 5.07}

stac_root = str(Path("example_stac_catalog/").absolute())

datacube = openeo.rest.datacube.DataCube(  # Syntax will be enhanced in future release of the Python client.
    openeo.rest.datacube.PGNode(
        "load_stac",
        arguments={
            "url": stac_root + "/collection.json",
            "temporal_extent": ["2023-06-01", "2023-06-09"],
            "spatial_extent": spatial_extent_tap,
        },
    ),
    connection=None,
)

# Scale values to make the output tiffs look good in standard image visualization tools:
datacube = datacube * (65534 / 2500)
datacube = datacube.linear_scale_range(0, 65534, 0, 65534)

output_dir = Path("tmp_local_output").absolute()
output_dir.mkdir(exist_ok=True)
datacube.print_json(file=output_dir / "process_graph.json", indent=2)

###################################
# Step 2, run process graph locally
###################################
containing_folder = Path(__file__).parent.absolute()
os.system(f"{containing_folder}/local_batch_job {output_dir / 'process_graph.json'} {containing_folder}")

# Note that output_dir / "collection.json" is a new stac collection and can be loaded in a new process graph.
