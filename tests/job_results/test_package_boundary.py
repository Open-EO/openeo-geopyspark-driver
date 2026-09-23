"""
Enforces that `openeogeotrellis.job_results` stays self-contained:

- it may not import other `openeogeotrellis` modules, `py4j`, `pyspark` or
  `geopyspark`, or call `get_jvm()`;
- imports inside the package are relative;
- nothing reads `get_backend_config()` or `ConfigParams()`.
"""
import ast
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "openeogeotrellis" / "job_results"

BANNED_ABSOLUTE_IMPORT_ROOTS = {"openeogeotrellis", "py4j", "pyspark", "geopyspark"}
BANNED_NAMES = {"get_backend_config", "ConfigParams", "get_jvm"}


def _package_py_files():
    return sorted(PACKAGE_ROOT.rglob("*.py"))


@pytest.mark.parametrize("path", _package_py_files(), ids=lambda p: str(p.relative_to(PACKAGE_ROOT)))
def test_no_banned_imports(path: Path):
    tree = ast.parse(path.read_text(), filename=str(path))

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                assert root not in BANNED_ABSOLUTE_IMPORT_ROOTS, (
                    f"{path}:{node.lineno}: banned absolute import of {alias.name!r}"
                )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                # Absolute import (not "from . import ..." / "from .x import ...").
                root = (node.module or "").split(".")[0]
                assert root not in BANNED_ABSOLUTE_IMPORT_ROOTS, (
                    f"{path}:{node.lineno}: banned absolute import from {node.module!r}"
                )


@pytest.mark.parametrize("path", _package_py_files(), ids=lambda p: str(p.relative_to(PACKAGE_ROOT)))
def test_no_banned_names(path: Path):
    tree = ast.parse(path.read_text(), filename=str(path))

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id in BANNED_NAMES:
            pytest.fail(f"{path}:{node.lineno}: banned name {node.id!r} used")
        if isinstance(node, ast.Attribute) and node.attr in BANNED_NAMES:
            pytest.fail(f"{path}:{node.lineno}: banned name {node.attr!r} used")


_ISOLATED_IMPORT_RUNNER = textwrap.dedent(
    """
    import importlib
    import pkgutil
    import sys

    class _BlockingFinder:
        blocked = {"py4j", "pyspark", "geopyspark", "openeogeotrellis"}

        def find_spec(self, fullname, path=None, target=None):
            root = fullname.split(".")[0]
            if root in self.blocked:
                raise ImportError(f"blocked import of {fullname!r} in isolated package test")
            return None

    sys.meta_path.insert(0, _BlockingFinder())

    package = importlib.import_module("__PACKAGE_NAME__")
    imported = []
    for module_info in pkgutil.walk_packages(package.__path__, prefix=package.__name__ + "."):
        importlib.import_module(module_info.name)
        imported.append(module_info.name)

    assert imported, "no submodules were found/imported"
    print("OK", *imported)
    """
)


def test_isolated_import(tmp_path):
    """
    Copies the package under a different parent package name, then imports
    every module in a subprocess where `py4j`/`pyspark`/`geopyspark`/
    `openeogeotrellis` are blocked. This verifies the directory still works
    unchanged when copied elsewhere.
    """
    isolated_parent = tmp_path / "isolated_parent"
    isolated_parent.mkdir()
    (isolated_parent / "__init__.py").write_text("")
    shutil.copytree(PACKAGE_ROOT, isolated_parent / "job_results")

    runner_script = tmp_path / "run_isolated_import.py"
    runner_script.write_text(_ISOLATED_IMPORT_RUNNER.replace("__PACKAGE_NAME__", "isolated_parent.job_results"))

    result = subprocess.run(
        [sys.executable, str(runner_script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, (
        f"isolated import failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout
