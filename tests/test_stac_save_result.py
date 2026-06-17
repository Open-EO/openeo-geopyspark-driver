import shutil
from pathlib import Path

from pystac import Collection

from openeogeotrellis.stac_save_result import (
    StacSaveResult,
)

repository_root = Path(__file__).parent.parent
print(repository_root)


def test_stac_save_result():
    tmp_dir = Path("tmp_stac_save_result").absolute()
    if tmp_dir.exists():
        # make sure the folder is empty
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()

    stac_root = "https://raw.githubusercontent.com/Open-EO/openeo-geopyspark-driver/refs/heads/master/docker/local_batch_job/example_stac_catalog/collection.json"
    sr = StacSaveResult(stac_root)
    ret = sr.write_assets(tmp_dir)
    Collection.from_file(sr.stac_root_local).validate_all()

    print(ret)
    for key, item in ret.items():
        for asset in item["assets"].values():
            assert Path(asset["href"]).exists()

    assert len(ret) == 3


def test_stac_save_result_recursive():
    tmp_dir = Path("tmp_stac_save_result_recursive").absolute()
    if tmp_dir.exists():
        # make sure the folder is empty
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()

    stac_root = str(repository_root / "tests/data/stac/recursive-stac-example/collection.json")
    sr = StacSaveResult(stac_root)
    ret = sr.write_assets(tmp_dir)
    Collection.from_file(sr.stac_root_local).validate_all()

    for key, item in ret.items():
        for asset in item["assets"].values():
            assert Path(asset["href"]).is_absolute()
            assert Path(asset["href"]).exists()

    print(ret)
    assert len(ret) == 3
