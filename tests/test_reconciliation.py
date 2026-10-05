"""Reconciliation tests: convergence, differences, alerts, EOD report.

Covers the two mandated flows: post-market reconciliation (report file +
read-only alert until acknowledged) and reconnect (reconcile-first resume,
alert on genuine drift).
"""

from __future__ import annotations

import json

import pytest
from fake_broker import FakeBrokerSession, MutableClock, epoch_of
from pulsar_contracts import ExecutionEventType, OrderStatus, Side
from conftest import make_intent, make_ts

from pulsar_exec.config import FeeSchedule
from pulsar_exec.fees import compute_fees
from pulsar_exec.live.gateway import GatewayState, MiniQMTGateway
from pulsar_exec.live.gate import LiveGate, LiveGateConfig
from pulsar_exec.live.reconcile import (
    DifferenceKind,
    Reconciler,
    run_post_market_reconciliation,
)

UNLOCK_ENV = "PULSAR_TEST_LIVE_UNLOCK"


def make_gateway(
    *,
    session: FakeBrokerSession | None = None,
    expected_initial_cash: float | None = None,
    baseline_positions: dict[str, int] | None = None,
    cash: float = 1_000_000.0,
) -> tuple[FakeBrokerSession, MiniQMTGateway]:
    session = session or FakeBrokerSession(cash=cash)
    if baseline_positions:
        for symbol, quantity in baseline_positions.items():
            session.set_position(symbol, quantity)
    clock = MutableClock(make_ts())
    config = LiveGateConfig(
        unlock_env=UNLOCK_ENV, max_order_value=10_000_000.0, max_daily_traded_value=10_000_000.0
    )
    gateway = MiniQMTGateway(
        session=session,
        gate=LiveGate(config, env={UNLOCK_ENV: "1"}, clock=clock),
        run_id="run-rec-0001",
        clock=clock,
        baseline_positions=baseline_positions,
        reconciler=Reconciler(expected_initial_cash=expected_initial_cash),
    )
    gateway.start()
    return session, gateway


def submit_and_get_wire(gateway: MiniQMTGateway, session: FakeBrokerSession, **kwargs):
    order_id = gateway.submit(make_intent(**kwargs))
    return order_id, session.wire_id_of_last_placed()


class TestConvergence:
    def test_silent_fill_discovered_by_poll(self) -> None:
        session, gateway = make_gateway()
        order_id, wire = submit_and_get_wire(gateway, session, quantity=100)
        # the fill happened while our callbacks were, say, dropped
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)), silent=True)
        assert gateway.query(order_id).status is OrderStatus.SUBMITTED
        gateway.poll()  # query-based convergence
        assert gateway.query(order_id).status is OrderStatus.FILLED
        assert gateway.query(order_id).filled_quantity == 100

    def test_reconnect_converges_then_resumes(self) -> None:
        session, gateway = make_gateway()
        order_id, wire = submit_and_get_wire(gateway, session, quantity=100)
        session.simulate_drop()
        assert gateway.state is GatewayState.DISCONNECTED
        # fill lands broker-side while we are blind
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)), silent=True)
        session.simulate_resume()
        # reconcile-first: the blind fill converged, book is clean -> resume
        assert gateway.state is GatewayState.ACCEPTING
        assert gateway.query(order_id).status is OrderStatus.FILLED
        assert gateway.submit(make_intent(seq=2, quantity=10)) is not None


class TestDifferences:
    def test_position_drift_alert_and_acknowledge(self) -> None:
        session, gateway = make_gateway(baseline_positions={"600519": 0})
        submit_and_get_wire(gateway, session, quantity=100)
        session.drift_position("600519", 50)  # broker moved shares, no trade
        session.simulate_drop()
        session.simulate_resume()
        assert gateway.state is GatewayState.READ_ONLY_ALERT
        assert gateway.alert_reason is not None
        assert DifferenceKind.POSITION_MISMATCH.value in gateway.alert_reason
        # new intents refused while the alert stands
        gateway.submit(make_intent(seq=3, quantity=10))
        # operator confirms after reviewing the report
        assert gateway.acknowledge_alert("manual transfer confirmed with broker")
        assert gateway.state is GatewayState.ACCEPTING

    def test_acknowledge_requires_note(self) -> None:
        _, gateway = make_gateway()
        with pytest.raises(ValueError):
            gateway.acknowledge_alert("")

    def test_order_unknown_at_broker_is_high(self) -> None:
        session, gateway = make_gateway()
        submit_and_get_wire(gateway, session)
        session.drop_order(session.wire_id_of_last_placed())
        report = gateway.reconcile()
        kinds = [entry.kind for entry in report.entries]
        assert DifferenceKind.ORDER_UNKNOWN_AT_BROKER in kinds
        assert not report.ok
        assert report.blocking

    def test_unconfirmed_submission_reported_not_assumed(self) -> None:
        session, gateway = make_gateway()
        session.unavailable = True
        order_id = gateway.submit(make_intent())
        session.unavailable = False
        report = gateway.reconcile()
        assert any(
            entry.kind is DifferenceKind.ORDER_UNKNOWN_AT_BROKER
            for entry in report.entries
        )
        # never assumed failed: still CREATED locally, never re-sent
        assert gateway.query(order_id).status is OrderStatus.CREATED

    def test_orphan_trade_and_manual_order_reported(self) -> None:
        session, gateway = make_gateway()
        session.inject_orphan_trade(
            99_999, "600519", 100, 1800.0, epoch_of(make_ts(1))
        )
        session.inject_orphan_order(99_998, "000001", 200)
        report = gateway.reconcile()
        kinds = {entry.kind for entry in report.entries}
        assert DifferenceKind.ORPHAN_TRADE in kinds
        assert DifferenceKind.ORPHAN_BROKER_ORDER in kinds

    def test_terminal_without_trade_records_deferred(self) -> None:
        session, gateway = make_gateway()
        submit_and_get_wire(gateway, session, quantity=100)
        wire = session.wire_id_of_last_placed()
        # broker claims a full fill but no trade records exist to
        # reconstruct it: the gateway refuses to fabricate the fill and the
        # report says the books disagree
        session._orders[wire].record = session._orders[wire].record.__class__(
            **{
                **session._orders[wire].record.__dict__,
                "filled_quantity": 100,
                "status_code": 52,
            }
        )
        report = gateway.reconcile()
        kinds = {entry.kind for entry in report.entries}
        assert DifferenceKind.FILL_QUANTITY_MISMATCH in kinds or (
            DifferenceKind.MISSING_TRADE_RECORDS in kinds
        )
        assert not report.ok


class TestPostMarketReport:
    def test_clean_report_written(self, tmp_path) -> None:
        session, gateway = make_gateway(baseline_positions={"600519": 0})
        _, wire = submit_and_get_wire(gateway, session, quantity=100)
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))
        session.set_position("600519", 100)
        target = tmp_path / "reports" / "eod.json"
        report = run_post_market_reconciliation(gateway, target)
        assert report.ok, report.summary()
        payload = json.loads(target.read_text())
        assert payload["ok"] is True
        assert payload["entries"] == []
        assert payload["run_id"] == "run-rec-0001"

    def test_dirty_report_written_and_blocks(self, tmp_path) -> None:
        session, gateway = make_gateway(baseline_positions={"600519": 0})
        _, wire = submit_and_get_wire(gateway, session, quantity=100)
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))
        session.set_position("600519", 60)  # 40 shares drifted
        target = tmp_path / "eod.json"
        report = run_post_market_reconciliation(gateway, target)
        assert not report.ok
        payload = json.loads(target.read_text())
        assert payload["ok"] is False
        assert payload["entries"][0]["kind"] == "position_mismatch"
        # the dirty report blocks the next live session until confirmed
        assert gateway.state is GatewayState.READ_ONLY_ALERT

    def test_cash_reconciliation(self) -> None:
        # matching cash: 1M + 0 fills
        session, gateway = make_gateway(expected_initial_cash=1_000_000.0)
        assert gateway.reconcile().ok
        # drifted cash
        session2, gateway2 = make_gateway(
            expected_initial_cash=500_000.0, cash=1_000_000.0
        )
        report = gateway2.reconcile()
        assert any(entry.kind is DifferenceKind.CASH_MISMATCH for entry in report.entries)

    def test_cash_matches_after_sell(self) -> None:
        session, gateway = make_gateway(
            expected_initial_cash=1_000_000.0,
            baseline_positions={"600519": 300},
        )
        _, wire = submit_and_get_wire(
            gateway, session, side=Side.SELL, quantity=100, limit_price=50.0
        )
        session.exchange_fill(wire, 100, 50.0, epoch_of(make_ts(1)))
        fees = compute_fees(
            price=50.0, quantity=100, side=Side.SELL, schedule=FeeSchedule()
        )
        session.asset = session.asset.__class__(
            cash=1_000_000.0 + 100 * 50.0 - fees.total,
            frozen_cash=0.0,
            market_value=0.0,
            total_asset=1_000_000.0,
        )
        report = gateway.reconcile()
        assert report.ok, report.summary()
