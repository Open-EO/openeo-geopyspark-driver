import sys
from pathlib import Path

import openeogeotrellis.deploy.local
from openeo.internal.graph_building import as_flat_graph
from openeo.util import ensure_dir


def run_graph_locally(process_graph, output_dir):
    openeogeotrellis.deploy.local.setup_environment()
    # Can only import after setup_environment:
    from openeogeotrellis.backend import JOB_METADATA_FILENAME
    from openeogeotrellis.deploy.batch_job import run_job
    output_dir = Path(output_dir)
    process_graph = as_flat_graph(process_graph)
    if "process_graph" not in process_graph:
        process_graph = {"process_graph": process_graph}
    run_job(
        process_graph,
        output_file=output_dir / "random_folder_name",
        metadata_file=output_dir / JOB_METADATA_FILENAME,
        api_version="2.0.0",
        job_dir=ensure_dir(output_dir),
        dependencies=[],
        user_id="jenkins",
    )


def main():
    """
    for setup.py entry_points
    """
    if len(sys.argv) < 2:
        print("Usage: run_graph_locally.py path/to/process_graph.json [path/to/output/]")
        sys.exit(1)
    process_graph_path = Path(sys.argv[1])
    if len(sys.argv) > 2:
        workdir = Path(sys.argv[2])
    else:
        workdir = process_graph_path.parent
    run_graph_locally(process_graph_path, workdir)


if __name__ == "__main__":
    main()
