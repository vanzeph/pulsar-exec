"""MiniQMTGateway tests against the in-memory protocol fake.

Covers the full live-channel contract: submission through the gate,
push-digested fills/cancels, idempotency, unconfirmed-outcome semantics
(never assumed failed), stale/unknown pushes, event archiving and the
gateway-level rejection of the three mandated gate classes.
"""

from __future__ import annotations

import pytest
from fake_broker import FakeBrokerSession, MutableClock, epoch_of
from pulsar_contracts import (
    ExecutionEventType,
    OrderStatus,
    PriceMode,
    Side,
)
from conftest import make_intent, make_ts

from pulsar_exec.live.archive import EventArchive
from pulsar_exec.live.gateway import GatewayState, MiniQMTGateway
from pulsar_exec.live.gate import GateRejectCode, LiveGate, LiveGateConfig

UNLOCK_ENV = "PULSAR_TEST_LIVE_UNLOCK"
BIG = 10_000_000.0


def make_config(**overrides: float | str) -> LiveGateConfig:
    params: dict[str, float | str] = {
        "unlock_env": UNLOCK_ENV,
        "max_order_value": BIG,
        "max_daily_traded_value": BIG,
    }
    params.update(overrides)
    return LiveGateConfig(**params)


def make_gateway(
    *,
    env: dict[str, str] | None = None,
    config: LiveGateConfig | None = None,
    session: FakeBrokerSession | None = None,
    tmp_dir=None,
    **kwargs,
) -> tuple[FakeBrokerSession, MiniQMTGateway, MutableClock, list]:
    session = session or FakeBrokerSession()
    clock = MutableClock(make_ts())
    archive = EventArchive(tmp_dir / "events") if tmp_dir else None
    events: list = []
    gateway = MiniQMTGateway(
        session=session,
        gate=LiveGate(
            config or make_config(),
            env=env if env is not None else {UNLOCK_ENV: "1"},
            clock=clock,
        ),
        run_id="run-live-0001",
        clock=clock,
        archive=archive,
        **kwargs,
    )
    gateway.on_event(events.append)
    gateway.start()
    return session, gateway, clock, events


def types_of(events: list) -> list:
    return [event.event_type for event in events]


def make_marketable_intent(seq: int, mode: PriceMode):
    """A marketable intent (no limit price) for gate/translation tests."""
    from pulsar_contracts import IdempotencyKey
    from conftest import TEST_RUN_ID

    return make_intent(seq=seq, limit_price=1800.0).model_copy(
        update={"price_mode": mode, "limit_price": None}
    )


class TestSubmitAndFill:
    def test_submit_accepted_and_forwarded(self) -> None:
        session, gateway, _, events = make_gateway()
        order_id = gateway.submit(make_intent())
        assert types_of(events) == [ExecutionEventType.ACCEPTED]
        assert gateway.query(order_id).status is OrderStatus.SUBMITTED
        assert session.placed_calls == [
            ("600519", 23, 300, 11, 1800.0)  # symbol, buy, qty, FIX price, limit
        ]
        assert gateway.wire_id_of(order_id) == session.wire_id_of_last_placed()

    def test_submit_is_idempotent_on_key(self) -> None:
        session, gateway, _, _ = make_gateway()
        intent = make_intent(seq=7)
        first = gateway.submit(intent)
        second = gateway.submit(intent)
        assert first == second
        assert len(session.placed_calls) == 1

    def test_partial_then_fill_via_push(self) -> None:
        session, gateway, clock, events = make_gateway()
        order_id = gateway.submit(make_intent(quantity=300))
        wire = session.wire_id_of_last_placed()
        base = epoch_of(make_ts(1))

        session.exchange_fill(wire, 100, 1799.5, base)
        assert gateway.query(order_id).status is OrderStatus.PARTIALLY_FILLED
        assert gateway.query(order_id).filled_quantity == 100

        session.exchange_fill(wire, 200, 1800.0, base + 60)
        state = gateway.query(order_id)
        assert state.status is OrderStatus.FILLED
        assert state.filled_quantity == 300
        assert state.avg_fill_price == pytest.approx(
            (100 * 1799.5 + 200 * 1800.0) / 300
        )
        assert types_of(events) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.PARTIAL_FILL,
            ExecutionEventType.FILL,
        ]
        # per-fill fees were booked from the shared fee model
        fill_events = [e for e in events if e.fill is not None]
        assert all(f.fill.commission > 0 for f in fill_events)
        assert all(f.fill.stamp_duty == 0.0 for f in fill_events)  # buys

    def test_sell_fill_carries_stamp_duty(self) -> None:
        session, gateway, _, events = make_gateway()
        gateway.submit(make_intent(side=Side.SELL, quantity=200, limit_price=50.0))
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 200, 50.0, epoch_of(make_ts(1)))
        fill = next(e for e in events if e.fill is not None).fill
        assert fill.stamp_duty > 0.0

    def test_positions_mapped_from_broker(self) -> None:
        from pulsar_exec.live.protocol import WirePosition

        session = FakeBrokerSession(
            positions=(
                WirePosition("600519", 1000, 800, avg_cost=1700.0),
                WirePosition("000001", 0, 0),
            )
        )
        _, gateway, _, _ = make_gateway(session=session)
        positions = gateway.positions()
        by_symbol = {p.symbol: p for p in positions}
        assert by_symbol["600519"].quantity == 1000
        assert by_symbol["600519"].available_quantity == 800
        assert by_symbol["600519"].avg_cost == pytest.approx(1700.0)

    def test_events_archived_to_jsonl(self, tmp_path) -> None:
        session, gateway, _, _ = make_gateway(tmp_dir=tmp_path)
        gateway.submit(make_intent(quantity=100))
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))
        archive = EventArchive(tmp_path / "events")
        rows = archive.read_lines()
        assert [row["event_type"] for row in rows] == ["accepted", "fill"]
        assert rows[1]["fill"]["quantity"] == 100


class TestCancel:
    def test_cancel_confirmed_via_push(self) -> None:
        session, gateway, _, events = make_gateway()
        order_id = gateway.submit(make_intent())
        wire = session.wire_id_of_last_placed()
        result = gateway.cancel(order_id)
        assert result.accepted
        assert gateway.query(order_id).status is OrderStatus.CANCELLED
        assert types_of(events) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.CANCELLED,
        ]

    def test_cancel_after_partial_fill(self) -> None:
        session, gateway, _, _ = make_gateway()
        order_id = gateway.submit(make_intent(quantity=300))
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 100, 1799.0, epoch_of(make_ts(1)))
        assert gateway.cancel(order_id).accepted
        assert gateway.query(order_id).status is OrderStatus.CANCELLED
        assert gateway.query(order_id).filled_quantity == 100

    def test_cancel_refused_by_broker(self) -> None:
        session, gateway, _, _ = make_gateway()
        order_id = gateway.submit(make_intent())
        wire = session.wire_id_of_last_placed()
        session.cancel_refused.add(wire)
        result = gateway.cancel(order_id)
        assert not result.accepted
        assert "refused" in (result.reason or "")
        assert gateway.query(order_id).status is OrderStatus.SUBMITTED

    def test_cancel_unknown_order(self) -> None:
        from pulsar_contracts import OrderId

        _, gateway, _, _ = make_gateway()
        result = gateway.cancel(OrderId("ord-nope"))
        assert not result.accepted
        assert result.reason == "unknown order_id"


class TestUnconfirmedOutcomes:
    def test_unavailable_submit_is_error_not_rejection(self) -> None:
        session, gateway, _, events = make_gateway()
        session.unavailable = True
        order_id = gateway.submit(make_intent())
        assert types_of(events) == [ExecutionEventType.ERROR]
        assert gateway.query(order_id).status is OrderStatus.CREATED
        assert order_id in gateway.unconfirmed_submissions
        # nothing was placed: no duplicate order can exist
        assert session.placed_calls == []

    def test_unavailable_cancel_leaves_state_unknown(self) -> None:
        session, gateway, _, _ = make_gateway()
        order_id = gateway.submit(make_intent())
        wire = session.wire_id_of_last_placed()
        session.cancel_unconfirmed.add(wire)
        result = gateway.cancel(order_id)
        assert not result.accepted
        assert "unconfirmed" in (result.reason or "")
        # the order is NOT assumed cancelled and NOT re-submitted
        assert gateway.query(order_id).status is OrderStatus.SUBMITTED
        assert len(session.placed_calls) == 1

    def test_unknown_wire_status_is_error_and_state_kept(self) -> None:
        session, gateway, _, events = make_gateway()
        order_id = gateway.submit(make_intent())
        wire = session.wire_id_of_last_placed()
        session._set_status(wire, 99, "alien status")
        assert ExecutionEventType.ERROR in types_of(events)
        assert gateway.query(order_id).status is OrderStatus.SUBMITTED


class TestGateAtGateway:
    def test_locked_gateway_rejects_with_trail(self) -> None:
        session, gateway, _, events = make_gateway(env={})
        order_id = gateway.submit(make_intent())
        assert types_of(events) == [ExecutionEventType.REJECTED]
        assert gateway.query(order_id).status is OrderStatus.REJECTED
        assert gateway.query(order_id).filled_quantity == 0
        codes = [r.code for r in gateway.gate.rejections]
        assert GateRejectCode.NOT_UNLOCKED in codes
        assert session.placed_calls == []

    def test_over_per_order_cap_rejected_at_gateway(self) -> None:
        _, gateway, _, events = make_gateway(
            config=make_config(max_order_value=100_000.0)
        )
        gateway.submit(make_intent())  # 540k notional
        assert types_of(events) == [ExecutionEventType.REJECTED]
        assert (
            gateway.gate.rejections[0].code is GateRejectCode.OVER_ORDER_LIMIT
        )

    def test_over_daily_cap_rejected_after_fills(self) -> None:
        session, gateway, _, events = make_gateway(
            config=make_config(max_daily_traded_value=400_000.0)
        )
        gateway.submit(make_intent(quantity=100))  # 180k notional
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))
        assert gateway.gate.day_traded_value == pytest.approx(180_000.0)
        # a further 360k order would put the day over the 400k budget
        gateway.submit(make_intent(seq=2, quantity=200))
        rejections = [r.code for r in gateway.gate.rejections]
        assert rejections[-1] is GateRejectCode.OVER_DAILY_LIMIT
        assert len(session.placed_calls) == 1

    def test_refused_order_is_rejected(self) -> None:
        session, gateway, _, events = make_gateway()
        session.refuse_orders = True
        gateway.submit(make_intent())
        assert types_of(events) == [ExecutionEventType.REJECTED]
        assert "negative acknowledgement" in (events[0].reason or "")

    def test_untranslatable_price_mode_rejected(self) -> None:
        _, gateway, _, events = make_gateway(
            reference_price_provider=lambda symbol: 1800.0
        )
        intent = make_marketable_intent(3, PriceMode.COUNTER_PRICE)
        gateway.submit(intent)
        assert types_of(events) == [ExecutionEventType.REJECTED]
        assert "no miniQMT wire translation" in (events[0].reason or "")

    def test_marketable_without_reference_price_rejected_unpriced(self) -> None:
        _, gateway, _, events = make_gateway()
        intent = make_marketable_intent(4, PriceMode.FIVE_LEVEL_CANCEL_REMAINDER)
        gateway.submit(intent)
        assert types_of(events) == [ExecutionEventType.REJECTED]
        assert (
            gateway.gate.rejections[-1].code is GateRejectCode.UNPRICED
        )


class TestPushRobustness:
    def test_duplicate_and_stale_pushes_absorbed(self) -> None:
        session, gateway, _, events = make_gateway()
        order_id = gateway.submit(make_intent(quantity=200))
        wire = session.wire_id_of_last_placed()
        trade = session.exchange_fill(wire, 200, 1800.0, epoch_of(make_ts(1)))
        assert gateway.query(order_id).status is OrderStatus.FILLED
        # replayed trade + late cancel push after terminal state
        session._push_trade(trade)
        session._set_status(wire, 54, "late cancel")
        assert gateway.query(order_id).status is OrderStatus.FILLED
        assert gateway.stale_push_count >= 1
        assert types_of(events) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.FILL,
        ]

    def test_overfilling_trade_not_applied(self) -> None:
        session, gateway, _, _ = make_gateway()
        order_id = gateway.submit(make_intent(quantity=100))
        wire = session.wire_id_of_last_placed()
        session.exchange_fill(wire, 100, 1800.0, epoch_of(make_ts(1)))
        # a corrupt broker record claiming more volume than the order
        from pulsar_exec.live.protocol import WireTrade

        session._trades["BAD1"] = WireTrade("BAD1", wire, "600519", 1800.0, 50, epoch_of(make_ts(2)))
        session._push_trade(session._trades["BAD1"])
        assert gateway.query(order_id).filled_quantity == 100
        assert gateway.stale_push_count >= 1

    def test_disconnected_gateway_refuses_and_recovers(self) -> None:
        session, gateway, _, events = make_gateway()
        gateway.submit(make_intent())
        session.simulate_drop()
        assert gateway.state is GatewayState.DISCONNECTED
        gateway.submit(make_intent(seq=2))
        assert [e.event_type for e in events][-1] is ExecutionEventType.REJECTED
        # clean truth -> reconnect reconciles and resumes
        session.simulate_resume()
        assert gateway.state is GatewayState.ACCEPTING
        gateway.submit(make_intent(seq=3))
        assert [e.event_type for e in events][-1] is ExecutionEventType.ACCEPTED
