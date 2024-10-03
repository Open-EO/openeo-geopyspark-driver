import os
from pathlib import Path

import openeo

# this example makes a small process graph and executes it in a local OpenEO container.

spatial_extent_tap = {"east": 5.08, "north": 51.22, "south": 51.215, "west": 5.07}

datacube = openeo.rest.datacube.DataCube(  # Syntax will be enhanced in future release of the Python client.
    openeo.rest.datacube.PGNode(
        "load_stac",
        arguments={
            "url": "https://artifactory.vgt.vito.be/artifactory/testdata-public/stac_example/collection.json",
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

containing_folder = Path(__file__).parent
os.system(f"{containing_folder}/local_batch_job {output_dir / 'process_graph.json'}")
