"""Import-purity guard: pulsar-exec semantics must stay channel- and I/O-free.

Per the Pulsar architecture baseline this package must not depend on
``pulsar-core``, on any concrete data-source/broker SDK, or on network
libraries — it only builds on ``pulsar-contracts``.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pulsar_exec

SRC_ROOT = Path(pulsar_exec.__file__).resolve().parent

FORBIDDEN_TOP_LEVEL = frozenset(
    {
        "pulsar_core",
        "backtrader",
        "vnpy",
        "xtquant",
        "easytrader",
        "tdx",
        "pytdx",
        "requests",
        "httpx",
        "aiohttp",
        "urllib3",
        "socket",
        "websocket",
    }
)


def _imported_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.add(node.module.split(".")[0])
    return names


def test_no_forbidden_imports_in_source() -> None:
    for path in sorted(SRC_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        offenders = _imported_names(tree) & FORBIDDEN_TOP_LEVEL
        assert not offenders, f"{path.name} imports forbidden modules: {offenders}"


def test_no_forbidden_modules_loaded_at_runtime() -> None:
    assert "pulsar_exec" in sys.modules
    offenders = FORBIDDEN_TOP_LEVEL & set(sys.modules)
    assert not offenders, f"forbidden modules loaded: {offenders}"


def test_only_pulsar_dependency_is_contracts() -> None:
    """All pulsar-* modules pulled in by pulsar_exec are contracts or itself."""
    pulsar_packages = {
        name.split(".")[0] for name in sys.modules if name.split(".")[0].startswith("pulsar")
    }
    assert pulsar_packages  # sanity: the scan actually saw modules
    assert pulsar_packages <= {"pulsar_exec", "pulsar_contracts"}, pulsar_packages
