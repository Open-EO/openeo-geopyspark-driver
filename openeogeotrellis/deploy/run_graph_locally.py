import json
import os
import sys
from datetime import datetime
from pathlib import Path


from openeogeotrellis.deploy.local import setup_environment
from openeo.internal.graph_building import as_flat_graph
from openeo.util import ensure_dir


def run_graph_locally(process_graph, output_dir):
    files_orig = set(output_dir.rglob("*"))
    process_start = datetime.now()
    output_dir = ensure_dir(output_dir)
    setup_environment(output_dir)
    # Can only import after setup_environment:
    from openeogeotrellis.backend import JOB_METADATA_FILENAME
    from openeogeotrellis.deploy.batch_job import run_job

    process_graph = as_flat_graph(process_graph)
    if "process_graph" not in process_graph:
        process_graph = {"process_graph": process_graph}
    run_job(
        process_graph,
        output_file=output_dir / "out",  # just like in backend.py
        metadata_file=output_dir / JOB_METADATA_FILENAME,
        api_version="2.0.0",
        job_dir=output_dir,
        dependencies=[],
        user_id="run_graph_locally",
    )
    # Set the permissions so any user can read and delete the files:
    # For when running inside a docker container.
    files_now = set(output_dir.rglob("*"))
    files_new = filter(lambda f: f not in files_orig or f.stat().st_mtime > process_start.timestamp(), files_now)
    for file in files_new:
        if file.is_file():
            os.chmod(file, 0o666)


def main():
    """
    for setup.py entry_points
    """
    if len(sys.argv) < 2:
        print("Usage: run_graph_locally.py path/to/process_graph.json [path/to/output/]")
        sys.exit(1)
    process_graph_path = Path(sys.argv[1])
    if len(sys.argv) > 2:
        output_dir = Path(sys.argv[2])
    else:
        output_dir = process_graph_path.parent

    if not "GEOPYSPARK_JARS_PATH" in os.environ:
        repository_root = Path(__file__).parent.parent.parent
        if os.path.exists(repository_root / "jars"):
            previous = (":" + os.environ["GEOPYSPARK_JARS_PATH"]) if "GEOPYSPARK_JARS_PATH" in os.environ else ""
            os.environ["GEOPYSPARK_JARS_PATH"] = str(repository_root / "jars") + previous
    run_graph_locally(process_graph_path, output_dir)


if __name__ == "__main__":
    main()
