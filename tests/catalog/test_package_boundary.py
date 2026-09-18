"""
Enforces that `openeogeotrellis.catalog` stays self-contained and engine-agnostic:

- absolute imports from `openeogeotrellis` are limited to a small allowlist
  (`openeogeotrellis.constants`, `openeogeotrellis.util.*`, `openeogeotrellis.opensearch`);
- `geopyspark`, `pyspark` and `py4j` may never be imported;
- imports between modules of the package itself must be relative;
- nothing reads `get_backend_config()` or `ConfigParams()`.

See ``layercatalog_decoupling/02-target-architecture.md`` §5 for the rules this test enforces.
"""
import ast
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "openeogeotrellis" / "catalog"

BANNED_ABSOLUTE_IMPORT_ROOTS = {"py4j", "pyspark", "geopyspark"}
ALLOWED_OPENEOGEOTRELLIS_MODULES = {"openeogeotrellis.constants", "openeogeotrellis.opensearch"}
ALLOWED_OPENEOGEOTRELLIS_PREFIXES = ("openeogeotrellis.util.", "openeogeotrellis.util")
BANNED_NAMES = {"get_backend_config", "ConfigParams"}


def _package_py_files():
    return sorted(PACKAGE_ROOT.rglob("*.py"))


def _is_allowed_openeogeotrellis_module(module: str) -> bool:
    return module in ALLOWED_OPENEOGEOTRELLIS_MODULES or module == "openeogeotrellis.util" or module.startswith(
        "openeogeotrellis.util."
    )


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
                if root == "openeogeotrellis":
                    assert _is_allowed_openeogeotrellis_module(alias.name), (
                        f"{path}:{node.lineno}: {alias.name!r} is not in the openeogeotrellis.catalog allowlist"
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                # Absolute import (not "from . import ..." / "from .x import ...").
                module = node.module or ""
                root = module.split(".")[0]
                assert root not in BANNED_ABSOLUTE_IMPORT_ROOTS, (
                    f"{path}:{node.lineno}: banned absolute import from {module!r}"
                )
                if root == "openeogeotrellis":
                    assert module == "openeogeotrellis" or _is_allowed_openeogeotrellis_module(module), (
                        f"{path}:{node.lineno}: {module!r} is not in the openeogeotrellis.catalog allowlist "
                        f"(intra-package imports must be relative)"
                    )
                    if module == "openeogeotrellis":
                        pytest.fail(
                            f"{path}:{node.lineno}: intra-package import of 'openeogeotrellis.catalog' "
                            f"must be relative"
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
        blocked = {"py4j", "pyspark", "geopyspark"}

        def find_spec(self, fullname, path=None, target=None):
            root = fullname.split(".")[0]
            if root in self.blocked:
                raise ImportError(f"blocked import of {fullname!r} in isolated package test")
            if fullname == "openeogeotrellis" or fullname == "openeogeotrellis._version":
                # Harmless package __init__ / version module, unconditionally triggered
                # by importing any openeogeotrellis submodule.
                return None
            if fullname.startswith("openeogeotrellis.") and fullname not in __ALLOWED_MODULES__:
                allowed_prefix = any(
                    fullname == m or fullname.startswith(m + ".") for m in __ALLOWED_MODULES__
                )
                if not allowed_prefix:
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
    Copies the package under a different parent package name, then imports every module in a
    subprocess where `py4j`/`pyspark`/`geopyspark` and non-allowlisted `openeogeotrellis` modules
    are blocked. This verifies the directory still works unchanged when copied elsewhere, and
    catches absolute intra-package imports that the AST scan above would wave through.
    """
    isolated_parent = tmp_path / "isolated_parent"
    isolated_parent.mkdir()
    (isolated_parent / "__init__.py").write_text("")
    shutil.copytree(PACKAGE_ROOT, isolated_parent / "catalog")

    allowed_modules = repr({"openeogeotrellis.constants", "openeogeotrellis.opensearch", "openeogeotrellis.util"})
    runner_script = tmp_path / "run_isolated_import.py"
    runner_script.write_text(
        _ISOLATED_IMPORT_RUNNER.replace("__PACKAGE_NAME__", "isolated_parent.catalog").replace(
            "__ALLOWED_MODULES__", allowed_modules
        )
    )

    env = dict(os.environ, PYTHONPATH=str(REPO_ROOT))
    result = subprocess.run(
        [sys.executable, str(runner_script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
    )

    assert result.returncode == 0, (
        f"isolated import failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert "OK" in result.stdout
