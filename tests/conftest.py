import os
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "local"))

import run_local  # noqa: E402


@pytest.fixture(scope="session")
def common_ns(tmp_path_factory):
    """Namespace with nb_common's helpers (no Spark needed for pure functions)."""
    os.environ["PORTFOLIO_LOCAL_ROOT"] = str(tmp_path_factory.mktemp("common"))
    g = {"__name__": "nb_common", "spark": None}
    for code, _ in run_local.parse_cells("nb_common"):
        exec(compile(code, "nb_common", "exec"), g)
    return g


@pytest.fixture(scope="session")
def spark():
    s = run_local.get_spark()
    yield s
