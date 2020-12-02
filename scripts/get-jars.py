"""

Script to download (custom) geotrellis backend assemly and geotrellis extensions jars

To be used instead of `geopyspark install-jar`

"""
import logging
from pathlib import Path
import subprocess
import urllib.request

logger = logging.getLogger("get-jars")


def ensure_jar_dir(jar_dir: Path) -> Path:
    logger.info("Checking jar dir {j}".format(j=jar_dir))
    if not jar_dir.exists():
        logger.info("Creating jar dir {j}".format(j=jar_dir))
        jar_dir.mkdir(parents=True)
    assert jar_dir.is_dir()
    return jar_dir


def download_jar(jar_dir: Path, url: str) -> Path:
    target = jar_dir / (url.split("/")[-1])
    if not target.exists():
        logger.info("Downloading {t} from {u!r}".format(t=target, u=url))
        urllib.request.urlretrieve(url, target)
        assert target.exists()
        logger.info("Got: {t}".format(t=target))
    else:
        logger.info("Already exists: {t}".format(t=target))
    return target


def main():
    logging.basicConfig(level=logging.INFO)

    root_dir = Path(__file__).parent.parent.absolute()
    jar_dir = ensure_jar_dir(root_dir / "jars")

    download_jar(
        jar_dir,
        "https://artifactory.vgt.vito.be/libs-snapshot-public/org/openeo/geotrellis-extensions/2.0.0-SNAPSHOT/geotrellis-extensions-2.0.0-SNAPSHOT.jar"
    )
    download_jar(
        jar_dir,
        "https://artifactory.vgt.vito.be/auxdata-public/openeo/geotrellis-backend-assembly-0.4.6-openeo.jar"
    )

    logger.info("Listing of {j}:".format(j=jar_dir))
    subprocess.call(["ls", "-al", str(jar_dir)])


if __name__ == '__main__':
    main()
