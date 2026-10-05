"""Safe-shutdown tests (the mandated 安全停机 scenario).

Shutdown must: refuse new intents (with a trail), cancel active orders,
record — never assume — unconfirmed cancels, converge via reconciliation
when possible, disconnect the session and persist the manifest.
"""

from __future__ import annotations

import json

from fake_broker import FakeBrokerSession, MutableClock, epoch_of
from pulsar_contracts import ExecutionEventType, OrderStatus
from conftest import make_intent, make_ts

from pulsar_exec.live.gateway import GatewayState, MiniQMTGateway
from pulsar_exec.live.gate import GateRejectCode, LiveGate, LiveGateConfig
from pulsar_exec.live.shutdown import safe_shutdown

UNLOCK_ENV = "PULSAR_TEST_LIVE_UNLOCK"


def make_gateway(
    *, session: FakeBrokerSession | None = None
) -> tuple[FakeBrokerSession, MiniQMTGateway, list]:
    session = session or FakeBrokerSession()
    clock = MutableClock(make_ts())
    config = LiveGateConfig(
        unlock_env=UNLOCK_ENV, max_order_value=10_000_000.0, max_daily_traded_value=10_000_000.0
    )
    events: list = []
    gateway = MiniQMTGateway(
        session=session,
        gate=LiveGate(config, env={UNLOCK_ENV: "1"}, clock=clock),
        run_id="run-sd-0001",
        clock=clock,
    )
    gateway.on_event(events.append)
    gateway.start()
    return session, gateway, events


class TestSafeShutdown:
    def test_shutdown_cancels_halts_and_persists_manifest(self, tmp_path) -> None:
        session, gateway, events = make_gateway()
        order_a = gateway.submit(make_intent(seq=1))  # will cancel cleanly
        gateway.submit(make_intent(seq=2, quantity=100))
        wire_b = session.wire_id_of_last_placed()
        session.exchange_fill(wire_b, 50, 1800.0, epoch_of(make_ts(1)))  # partial
        session.cancel_unconfirmed.add(wire_b)  # order B cancel goes dark

        manifest = safe_shutdown(
            gateway, reason="core engine exception", manifest_dir=tmp_path
        )

        # 1. new intents are refused and left on the gate trail
        assert gateway.state is GatewayState.HALTED
        gateway.submit(make_intent(seq=3))
        last = events[-1]
        assert last.event_type is ExecutionEventType.REJECTED
        assert "halted" in (last.reason or "")
        assert gateway.gate.rejections[-1].code is GateRejectCode.GATE_HALTED

        # 2. the cancellable order was cancelled
        assert gateway.query(order_a).status is OrderStatus.CANCELLED

        # 3. the unconfirmed cancel was recorded, never assumed: order B
        #    stays PARTIALLY_FILLED (not assumed cancelled, not re-sent)
        assert manifest.unresolved, "unconfirmed cancel must surface as unresolved"
        unresolved_ids = [snap.order_id for snap in manifest.unresolved]
        assert str(order_a) not in unresolved_ids
        assert any(snap.cancel_outcome == "unconfirmed" for snap in manifest.orders)
        assert not manifest.clean
        assert {snap.status for snap in manifest.unresolved} == {"partially_filled"}

        # 4. manifest persisted with the full state (written before the
        #    post-shutdown rejection above, hence its own count)
        files = list(tmp_path.glob("shutdown-run-sd-0001-*.json"))
        assert len(files) == 1
        payload = json.loads(files[0].read_text())
        assert payload["reason"] == "core engine exception"
        assert payload["gateway_state"] == "halted"
        assert payload["unresolved"], payload
        assert payload["rejection_count"] == 0

        # 5. session closed
        assert not session.is_connected()

    def test_clean_shutdown_leaves_no_unresolved(self, tmp_path) -> None:
        session, gateway, _ = make_gateway()
        gateway.submit(make_intent(seq=1))
        gateway.submit(make_intent(seq=2, quantity=100))
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))

        manifest = safe_shutdown(gateway, reason="session end", manifest_dir=tmp_path)
        assert manifest.clean
        assert manifest.unresolved == ()
        assert all(
            snap.status in ("cancelled", "filled") for snap in manifest.orders
        )
        payload = json.loads(
            next(tmp_path.glob("shutdown-*.json")).read_text()
        )
        assert payload["clean"] is True
        assert payload["unresolved"] == []

    def test_shutdown_with_session_down_defers_reconciliation(self, tmp_path) -> None:
        session, gateway, _ = make_gateway()
        gateway.submit(make_intent(seq=1))
        session.unavailable = True  # broker unreachable mid-shutdown

        manifest = safe_shutdown(gateway, reason="network loss", manifest_dir=tmp_path)
        assert manifest.reconciliation_deferred
        # the active order stays unresolved-but-recorded, never assumed
        assert not manifest.clean
        assert all(
            snap.cancel_outcome == "unconfirmed" for snap in manifest.unresolved
        )

    def test_manifest_requires_reason(self) -> None:
        _, gateway, _ = make_gateway()
        import pytest

        with pytest.raises(ValueError):
            safe_shutdown(gateway, reason="")
