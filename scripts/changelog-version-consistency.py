# TODO: move this script to a repo that allow better reuse?

import re
import textwrap
from pathlib import Path


def get_version(path: Path) -> str:
    """Get version from _version.py"""
    versions = re.findall(r"__version__\s*=\s*['\"](.*?)['\"]", path.read_text(encoding="utf8"))
    if len(versions) != 1:
        raise ValueError(f"Expected one version, but found {versions}")
    return versions[0]


def get_in_progress_header(path: Path) -> str:
    """Get 'In progress' header from CHANGELOG.md"""
    in_progress_headers = re.findall(
        r"^## In progress: .*$", path.read_text(encoding="utf8"), re.MULTILINE | re.IGNORECASE
    )
    if len(in_progress_headers) != 1:
        raise ValueError(f"Expected single 'In progress' header, but found {in_progress_headers}")
    return in_progress_headers[0]


def main():
    root = Path.cwd()

    version_path = root / "openeogeotrellis" / "_version.py"
    expected_version = get_version(version_path)
    expected_version = expected_version.partition("a")[0]
    expected_in_progress_header = f"## In progress: {expected_version}"

    changelog_path = root / "CHANGELOG.md"
    in_progress_header = get_in_progress_header(path=changelog_path)

    if in_progress_header.lower() != expected_in_progress_header.lower():
        print(
            textwrap.dedent(
                f"""\
                Version inconsistency in CHANGELOG.md.
                Expected:
                    {expected_in_progress_header}
                but found:
                    {in_progress_header}
                """
            )
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
