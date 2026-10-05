"""xt_bridge tests: environment-only configuration and absent-SDK errors.

The bridge is the only module allowed to reference xtquant (dynamically);
on machines without the miniQMT terminal it must fail loudly and must
never leak the SDK import into ``pulsar_exec`` itself.
"""

from __future__ import annotations

import sys

import pytest


def test_importing_live_package_never_loads_xtquant() -> None:
    import pulsar_exec  # noqa: F401
    import pulsar_exec.live  # noqa: F401
    import pulsar_exec.live.xt_bridge  # noqa: F401

    assert "xtquant" not in sys.modules


def test_missing_account_env_is_refused_before_sdk_load() -> None:
    from pulsar_exec.live.xt_bridge import open_broker_session

    with pytest.raises(ValueError, match="PULSAR_MINIQMT_ACCOUNT_ID"):
        open_broker_session(
            userdata_path="/opt/miniqmt/userdata",
            env={},
        )


def test_missing_userdata_is_refused() -> None:
    from pulsar_exec.live.xt_bridge import open_broker_session

    with pytest.raises(ValueError, match="userdata"):
        open_broker_session(
            env={"PULSAR_MINIQMT_ACCOUNT_ID": "1234567890", "PULSAR_MINIQMT_USERDATA": ""}
        )


def test_absent_xtquant_raises_miniqmt_unavailable() -> None:
    from pulsar_exec.live.xt_bridge import (
        MiniQMTUnavailableError,
        open_broker_session,
    )

    if "xtquant" in sys.modules:  # pragma: no cover - terminal machines
        pytest.skip("xtquant importable on this machine")

    with pytest.raises(MiniQMTUnavailableError, match="miniQMT terminal"):
        open_broker_session(
            userdata_path="/opt/miniqmt/userdata",
            env={"PULSAR_MINIQMT_ACCOUNT_ID": "1234567890"},
        )
    # the failed attempt still did not import the SDK
    assert "xtquant" not in sys.modules
