import subprocess
import sys
from pathlib import Path


def test_local_batch_job_example():
    repository_root = Path(__file__).parent.parent
    cmd = [
        sys.executable,
        str(repository_root / "docker/local_batch_job/local_batch_job_example.py"),
    ]
    subprocess.check_output(cmd)
    output_file_example = repository_root / "docker/local_batch_job/tmp_local_output/openEO_2023-06-01Z.tif"
    assert output_file_example.exists()
