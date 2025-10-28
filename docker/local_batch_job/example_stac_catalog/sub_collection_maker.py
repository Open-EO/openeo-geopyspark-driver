#!/usr/bin/env python3
import json

import os
import shutil
import sys
from pathlib import Path
from typing import List

containing_folder = Path(__file__).parent


def copy_matching_files(source_folder: Path, target_folder: Path, request_date: str) -> List[Path]:
    assert source_folder.exists() and source_folder.is_dir()

    target_folder.mkdir(parents=True, exist_ok=True)

    copied_files = []
    glob_str = "*" + request_date + "*.tif*"
    # avoid "Invalid pattern: '**' can only be an entire path component"
    glob_str = glob_str.replace("**", "*").replace("**", "*").replace("**", "*")
    for file_path in source_folder.glob(glob_str):
        target_file_path = target_folder / file_path.name
        shutil.copy(file_path, target_file_path)
        copied_files.append(target_file_path)
        print(f"Copied to {target_file_path}")

    return copied_files


def main(argv: List[str]) -> None:
    print(f"Running sub_collection_maker with args: {argv}")
    if containing_folder == Path.cwd():
        output_folder = containing_folder / "tmp_sub_collection_output"
        print(
            "Warning: When running from CWL, cwd should be the output folder, "
            "and different than the source data folder. "
            "For testing purposes, using " + str(output_folder)
        )
        output_folder.mkdir(exist_ok=True)
        os.chdir(output_folder)

    target_folder = Path.cwd()
    request_date = argv[0]

    copied_files = copy_matching_files(containing_folder, target_folder, request_date)

    # sleep to check if it runs in parallel:
    # import time
    # time.sleep(60)

    collection_json = json.loads((containing_folder / "collection.json").read_text())
    copied_file_names = {f.name for f in copied_files}
    collection_json["links"] = [
        link for link in collection_json["links"] if Path(link["href"]).name in copied_file_names
    ]
    (target_folder / "collection.json").write_text(json.dumps(collection_json, indent=2))


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        # request_date = "*"
        request_date = "2023-06-01"
        print(f"Using default date {request_date} for testing purposes.")
        main(["2023-06-01"])
    else:
        main(sys.argv[1:])
