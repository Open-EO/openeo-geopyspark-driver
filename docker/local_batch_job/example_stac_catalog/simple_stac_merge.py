#!/usr/bin/env python3
import json
import os
import shutil
import sys
from pathlib import Path
from typing import List, Optional

containing_folder = Path(__file__).parent


def get_files_from_stac_catalog(catalog_path: Path):
    """
    Simple function that recursively searches files in catalog
    """
    catalog_path = Path(catalog_path)
    assert catalog_path.exists()
    catalog_json = json.loads(catalog_path.read_text())
    all_files = []
    links = []
    if "links" in catalog_json:
        links.extend(catalog_json["links"])
    if "assets" in catalog_json:
        links.extend(list(catalog_json["assets"].values()))
    for link in links:
        if "href" in link:
            href = link["href"]
            if href.startswith("file://"):
                href = href[7:]
            href = Path(os.path.normpath(os.path.join(catalog_path.parent, href)))
            all_files.append(href)

            if "rel" in link and (link["rel"] == "child" or link["rel"] == "item"):
                all_files.extend(get_files_from_stac_catalog(href))
    return all_files


def main(argv: List[str]) -> None:
    print(f"Running simple_stac_merge with args: {argv}")
    if containing_folder == Path.cwd():
        output_folder = containing_folder / "tmp_simple_stac_merge"
        output_folder.mkdir(exist_ok=True)
        print(
            "Warning: When running from CWL, cwd should be the output folder, "
            "and different than the source data folder. "
            "For testing purposes, using " + str(output_folder)
        )
    else:
        output_folder = Path.cwd()

    if len(argv) < 2:
        import sub_collection_maker

        tmp_sub_collection_output_1 = containing_folder / "tmp_sub_collection_output_1"
        tmp_sub_collection_output_1.mkdir(exist_ok=True)
        os.chdir(tmp_sub_collection_output_1)
        sub_collection_maker.main(["2023-06-01"])

        tmp_sub_collection_output_2 = containing_folder / "tmp_sub_collection_output_2"
        tmp_sub_collection_output_2.mkdir(exist_ok=True)
        os.chdir(tmp_sub_collection_output_2)
        sub_collection_maker.main(["2023-06-04"])

        input_arguments = [
            "S1_2images_collection.json",
            str(tmp_sub_collection_output_1),
            str(tmp_sub_collection_output_2),
        ]
        print(f"Using defaults tmp_sub_collection_output_1 / tmp_sub_collection_output_2 for testing purposes.")
    else:
        input_arguments = argv[1:]
        os.chdir(output_folder)
    collection_filename = list(filter(lambda x: x.endswith(".json"), input_arguments))
    if collection_filename:
        collection_filename = collection_filename[0]
    else:
        collection_filename = "collection.json"
    input_directories = list(filter(lambda x: not x.endswith(".json"), input_arguments))
    input_directories = [Path(c) for c in input_directories]

    collections = [d / "collection.json" for d in input_directories]
    first_json_path = collections[0]

    first_json = json.loads(first_json_path.read_text())
    for other_json_path in collections[1:]:
        other_json = json.loads(other_json_path.read_text())
        first_json["links"].extend(other_json["links"])
        if first_json["extent"] != other_json["extent"]:
            print(f"Warning: extents differ: {first_json['extent']} vs {other_json['extent']}. Using the first one.")

    for collection_path in collections:
        files = get_files_from_stac_catalog(collection_path)
        for f in files:
            # TODO: avoid overwriting same filenames
            # TODO: Keep folder structure. But right now we only get flat files here.
            shutil.copy(f, output_folder / f.name)

    (output_folder / "collection.json").write_text(json.dumps(first_json, indent=2))
    # TODO: Remove duplicate collection output once sar_coherence is fixed.
    (output_folder / collection_filename).write_text(json.dumps(first_json, indent=2))

    try:
        print("Trying pystac validation...")
        import logging
        from pystac import Collection, Item

        logging.basicConfig(level=logging.DEBUG)

        collection = Collection.from_file(output_folder / "collection.json")
        collection.validate_all()
        print("pystac validation successful")
    except Exception as e:
        print("pystac validation failed: " + str(e))


if __name__ == "__main__":
    main(sys.argv)
