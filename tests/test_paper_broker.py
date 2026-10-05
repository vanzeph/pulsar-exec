"""``PaperBroker`` behavioural tests: strict book matching, shared ledger,
session lifecycle and the intraday smoke run.

The paper channel reuses the backtest's components (account, state
machine, pre-trade validation, price-limit rules), so these tests focus
on what is paper-specific: strict (penetration) book matching, displayed
liquidity consumption, snapshot sequence semantics (late/gap tolerance),
and the start/stop session lifecycle with its event trail.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from pulsar_contracts import (
    Board,
    Exchange,
    ExecutionEvent,
    ExecutionEventType,
    ExecutionPort,
    IdempotencyKey,
    Instrument,
    InstrumentStatus,
    OrderIntent,
    PriceMode,
    QuoteLevel,
    SHANGHAI_TZ,
    Side,
    Snapshot,
    TimeInForce,
)

from pulsar_exec import (
    EventArchive,
    IdempotencyConflictError,
    MatchingRules,
    PaperBroker,
    PaperSessionState,
)

DAY1 = date(2026, 10, 5)
DAY2 = date(2026, 10, 6)

MAIN_INSTRUMENT = Instrument(
    symbol="600519",
    exchange=Exchange.SSE,
    board=Board.MAIN,
    list_date=date(2001, 8, 27),
)

SUSPENDED_INSTRUMENT = MAIN_INSTRUMENT.model_copy(
    update={"status": InstrumentStatus.SUSPENDED}
)


def ts(minute: int, day: date = DAY1) -> datetime:
    """Shanghai wall time on ``day``; contracts normalize the timezone."""
    return datetime(day.year, day.month, day.day, 9, 30, 0) + timedelta(
        minutes=minute
    )


def snap(
    seq: int,
    minute: int,
    last_price: float,
    *,
    asks: tuple[tuple[float, float], ...] = (),
    bids: tuple[tuple[float, float], ...] = (),
    symbol: str = "600519",
    day: date = DAY1,
) -> Snapshot:
    """A five-level snapshot; level tuples are ``(price, volume)``."""
    return Snapshot(
        symbol=symbol,
        ts=ts(minute, day),
        seq=seq,
        last_price=last_price,
        volume=100_000.0,
        amount=last_price * 100_000.0,
        bids=tuple(QuoteLevel(price=price, volume=volume) for price, volume in bids),
        asks=tuple(QuoteLevel(price=price, volume=volume) for price, volume in asks),
    )


def intent(
    seq: int,
    side: Side = Side.BUY,
    quantity: int = 1000,
    limit_price: float | None = 10.00,
    *,
    symbol: str = "600519",
    price_mode: PriceMode = PriceMode.LIMIT,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> OrderIntent:
    return OrderIntent(
        idempotency_key=IdempotencyKey(run_id="run-e3", seq=seq),
        side=side,
        symbol=symbol,
        quantity=quantity,
        price_mode=price_mode,
        limit_price=limit_price,
        time_in_force=time_in_force,
    )


def make_broker(**overrides) -> PaperBroker:
    """A started paper broker with strict rules and a set clock."""
    defaults: dict[str, object] = {
        "initial_cash": 1_000_000.0,
        "matching": MatchingRules(strict_price_boundary=True),
        "instruments": [MAIN_INSTRUMENT],
        "clock": ts(-15),  # 09:15, pre-open
    }
    defaults.update(overrides)
    broker = PaperBroker(**defaults)  # type: ignore[arg-type]
    broker.start(now=ts(-15))
    return broker


class Recorder:
    """Collects every execution event pushed by the broker."""

    def __init__(self, broker: PaperBroker) -> None:
        self.events: list[ExecutionEvent] = []
        broker.on_event(self.events.append)

    def of(self, order_id) -> list[ExecutionEvent]:
        return [event for event in self.events if event.order_id == order_id]

    def types_of(self, order_id) -> list[ExecutionEventType]:
        return [event.event_type for event in self.of(order_id)]

    def fills(self, order_id):
        return [
            event.fill
            for event in self.of(order_id)
            if event.event_type
            in (ExecutionEventType.PARTIAL_FILL, ExecutionEventType.FILL)
        ]


# ============================================================================
# Construction guards
# ============================================================================
class TestConstruction:
    def test_refuses_non_strict_rules(self):
        with pytest.raises(ValueError, match="strict"):
            PaperBroker(matching=MatchingRules(strict_price_boundary=False))

    def test_refuses_non_positive_cash(self):
        with pytest.raises(ValueError, match="initial_cash"):
            PaperBroker(initial_cash=0.0)

    def test_implements_execution_port(self):
        assert isinstance(make_broker(), ExecutionPort)


# ============================================================================
# Session lifecycle
# ============================================================================
class TestSessionLifecycle:
    def test_submit_before_start_is_rejected(self):
        broker = PaperBroker(
            matching=MatchingRules(strict_price_boundary=True), clock=ts(-15)
        )
        recorder = Recorder(broker)
        assert broker.state is PaperSessionState.IDLE

        order_id = broker.submit(intent(1))

        assert broker.query(order_id).status.value == "rejected"
        reason = recorder.of(order_id)[0].reason
        assert reason is not None and "idle" in reason

    def test_start_is_idempotent_and_recorded(self):
        broker = PaperBroker(
            matching=MatchingRules(strict_price_boundary=True), clock=ts(-15)
        )
        broker.start()
        broker.start()
        assert broker.state is PaperSessionState.RUNNING
        assert [record.kind for record in broker.session_trail()] == ["started"]

    def test_stop_cancels_active_orders_and_is_terminal(self):
        broker = make_broker()
        recorder = Recorder(broker)
        order_id = broker.submit(intent(1, Side.BUY, 1000, 9.00))  # rests

        broker.stop("end of smoke")

        assert broker.state is PaperSessionState.STOPPED
        assert broker.query(order_id).status.value == "cancelled"
        reason = recorder.of(order_id)[-1].reason
        assert reason is not None and "paper session stopped: end of smoke" in reason
        kinds = [record.kind for record in broker.session_trail()]
        assert kinds == ["started", "stopped"]
        assert broker.session_trail()[-1].detail == "end of smoke"

    def test_stop_is_idempotent(self):
        broker = make_broker()
        broker.stop("first")
        broker.stop("second")
        details = [r.detail for r in broker.session_trail() if r.kind == "stopped"]
        assert details == ["first"]

    def test_start_after_stop_is_refused(self):
        broker = make_broker()
        broker.stop("done")
        with pytest.raises(RuntimeError, match="already stopped"):
            broker.start()

    def test_submit_after_stop_is_rejected(self):
        broker = make_broker()
        recorder = Recorder(broker)
        broker.stop("done")

        order_id = broker.submit(intent(1))

        assert broker.query(order_id).status.value == "rejected"
        reason = recorder.of(order_id)[0].reason
        assert reason is not None and "stopped" in reason

    def test_snapshots_outside_a_running_session_are_ignored(self):
        broker = PaperBroker(
            matching=MatchingRules(strict_price_boundary=True), clock=ts(-15)
        )
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))
        assert broker.clock == ts(-15)  # the clock did not move

        broker.start()
        broker.stop("done")
        broker.on_snapshot(snap(2, 1, 9.94, asks=((9.94, 5000),)))
        assert broker.clock == ts(-15)


# ============================================================================
# Strict book matching (严格模式按买卖档位判定)
# ============================================================================
class TestStrictBoundary:
    def test_buy_touching_the_limit_does_not_fill(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(snap(1, 0, 10.00, asks=((10.00, 5000),)))

        assert broker.query(order_id).status.value == "submitted"
        assert broker.query(order_id).filled_quantity == 0

    def test_buy_fills_when_the_ask_penetrates_the_limit(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(snap(1, 0, 9.99, asks=((9.99, 5000),)))

        assert broker.query(order_id).status.value == "filled"
        assert broker.query(order_id).avg_fill_price == pytest.approx(9.99)

    def test_sell_touching_the_limit_does_not_fill(self):
        broker = make_broker()
        buy = broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))
        broker.on_session_end(DAY1)  # roll T+1: the 1000 shares sellable

        sell = broker.submit(intent(2, Side.SELL, 1000, 10.50))
        broker.on_snapshot(
            snap(2, 0, 10.50, bids=((10.50, 5000),), asks=((10.51, 5000),), day=DAY2)
        )

        assert broker.query(sell).status.value == "submitted"
        assert broker.query(sell).filled_quantity == 0
        assert broker.query(buy).status.value == "filled"

    def test_sell_fills_when_the_bid_penetrates_the_limit(self):
        broker = make_broker()
        broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))
        broker.on_session_end(DAY1)

        sell = broker.submit(intent(2, Side.SELL, 1000, 10.50))
        broker.on_snapshot(
            snap(2, 0, 10.51, bids=((10.51, 5000),), asks=((10.52, 5000),), day=DAY2)
        )

        assert broker.query(sell).status.value == "filled"
        assert broker.query(sell).avg_fill_price == pytest.approx(10.51)


class TestLevelSweep:
    def test_limit_buy_sweeps_crossable_levels_as_one_blended_fill(self):
        broker = make_broker()
        recorder = Recorder(broker)
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(
            snap(1, 0, 9.98, asks=((9.98, 300), (9.96, 700), (10.05, 9000)))
        )

        assert broker.query(order_id).status.value == "filled"
        assert recorder.types_of(order_id) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.FILL,
        ]
        # volume-weighted blend of 300 @ 9.98 + 700 @ 9.96, one fill event
        assert broker.query(order_id).avg_fill_price == pytest.approx(
            (300 * 9.98 + 700 * 9.96) / 1000
        )

    def test_orders_share_the_displayed_liquidity_fifo(self):
        broker = make_broker()
        first = broker.submit(intent(1, Side.BUY, 500, 10.00))
        second = broker.submit(intent(2, Side.BUY, 500, 10.00))

        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 700),)))

        assert broker.query(first).status.value == "filled"
        assert broker.query(second).status.value == "partially_filled"
        assert broker.query(second).filled_quantity == 200

        # the next snapshot refreshes the displayed book and completes it
        broker.on_snapshot(snap(2, 1, 9.94, asks=((9.94, 500),)))
        assert broker.query(second).status.value == "filled"


class TestMarketableIntents:
    def test_counter_price_takes_the_best_level_per_snapshot(self):
        broker = make_broker()
        # a first snapshot establishes the reference market price
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 400),)))
        order_id = broker.submit(
            intent(2, Side.BUY, 1000, None, price_mode=PriceMode.COUNTER_PRICE)
        )

        broker.on_snapshot(snap(2, 1, 9.95, asks=((9.95, 400), (9.90, 9000))))
        assert broker.query(order_id).status.value == "partially_filled"
        assert broker.query(order_id).filled_quantity == 400  # best level only

        broker.on_snapshot(snap(3, 2, 9.93, asks=((9.93, 9000),)))
        assert broker.query(order_id).status.value == "filled"
        assert broker.query(order_id).avg_fill_price == pytest.approx(
            (400 * 9.95 + 600 * 9.93) / 1000
        )

    def test_five_level_ioc_cancels_remainder_on_the_snapshot(self):
        broker = make_broker()
        broker.on_snapshot(snap(1, 0, 10.01, asks=((10.01, 100),)))
        recorder = Recorder(broker)
        order_id = broker.submit(
            intent(
                2, Side.BUY, 1000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER
            )
        )

        broker.on_snapshot(snap(2, 1, 10.01, asks=((10.01, 200), (10.02, 300))))

        assert recorder.types_of(order_id) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.PARTIAL_FILL,
            ExecutionEventType.CANCELLED,
        ]
        assert broker.query(order_id).filled_quantity == 500

    def test_five_level_ioc_with_empty_book_cancels_immediately(self):
        broker = make_broker()
        broker.on_snapshot(snap(1, 0, 10.00, asks=((10.00, 100),)))
        recorder = Recorder(broker)
        order_id = broker.submit(
            intent(
                2, Side.BUY, 1000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER
            )
        )

        broker.on_snapshot(snap(2, 1, 10.00, bids=((9.99, 1000),)))

        assert recorder.types_of(order_id) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.CANCELLED,
        ]


# ============================================================================
# Sealed one-line boards and suspensions (reused E2 rules)
# ============================================================================
class TestSealedBoard:
    def test_sealed_limit_up_blocks_a_sell_that_would_cross(self):
        broker = make_broker()
        broker.set_previous_close("600519", 10.00)
        broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.98, asks=((9.98, 5000),)))
        broker.on_session_end(DAY1)  # prev close becomes 9.98 -> band [8.98, 10.98]

        sell = broker.submit(intent(2, Side.SELL, 1000, 10.50))
        # sealed at the limit-up price 10.98: even a displayed bid through
        # the sell limit must not fill (一字板不可成交)
        broker.on_snapshot(
            snap(2, 0, 10.98, bids=((10.98, 999_999),), asks=(), day=DAY2)
        )
        assert broker.query(sell).status.value == "submitted"
        assert broker.query(sell).filled_quantity == 0

        # the market unlocks: the same sell now fills
        broker.on_snapshot(
            snap(3, 1, 10.80, bids=((10.80, 5000),), asks=((10.81, 5000),), day=DAY2)
        )
        assert broker.query(sell).status.value == "filled"

    def test_sealed_limit_down_blocks_a_buy_that_would_cross(self):
        broker = make_broker()
        broker.set_previous_close("600519", 10.00)  # band [9.00, 11.00]

        buy = broker.submit(intent(1, Side.BUY, 1000, 9.50))
        # sealed at the limit-down price 9.00: even a displayed ask through
        # the buy limit must not fill
        broker.on_snapshot(snap(1, 0, 9.00, asks=((9.00, 999_999),), bids=()))
        assert broker.query(buy).status.value == "submitted"
        assert broker.query(buy).filled_quantity == 0


class TestSuspension:
    def test_marked_suspended_symbol_never_fills(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.mark_suspended("600519", DAY1)
        assert broker.is_suspended("600519", DAY1)

        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))

        assert broker.query(order_id).status.value == "submitted"

    def test_suspended_instrument_rejects_submission(self):
        broker = make_broker(instruments=[SUSPENDED_INSTRUMENT])
        recorder = Recorder(broker)

        order_id = broker.submit(intent(1))

        assert broker.query(order_id).status.value == "rejected"
        reason = recorder.of(order_id)[0].reason
        assert reason is not None and "停牌" in reason


# ============================================================================
# Shared pre-trade validation (identical to the venue's)
# ============================================================================
class TestPreTradeValidation:
    def test_buy_must_be_board_lot(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 150, 10.00))
        assert broker.query(order_id).status.value == "rejected"

    def test_insufficient_funds_rejected(self):
        broker = make_broker(initial_cash=5_000.0)
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))
        assert broker.query(order_id).status.value == "rejected"

    def test_marketable_buy_without_any_market_price_rejected(self):
        broker = make_broker()
        order_id = broker.submit(
            intent(1, Side.BUY, 1000, None, price_mode=PriceMode.COUNTER_PRICE)
        )
        assert broker.query(order_id).status.value == "rejected"

    def test_limit_price_outside_band_rejected(self):
        broker = make_broker()
        broker.set_previous_close("600519", 10.00)  # band [9.00, 11.00]

        too_high = broker.submit(intent(1, Side.BUY, 1000, 11.50))
        too_low = broker.submit(intent(2, Side.BUY, 1000, 8.99))
        boundary = broker.submit(intent(3, Side.BUY, 1000, 11.00))

        assert broker.query(too_high).status.value == "rejected"
        assert broker.query(too_low).status.value == "rejected"
        assert broker.query(boundary).status.value == "submitted"

    def test_same_day_sell_rejected_until_the_day_rolls(self):
        broker = make_broker()
        recorder = Recorder(broker)
        buy = broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))

        same_day = broker.submit(intent(2, Side.SELL, 1000, 10.00))
        assert broker.query(same_day).status.value == "rejected"
        reason = recorder.of(same_day)[0].reason
        assert reason is not None and "T+1 available" in reason

        broker.on_session_end(DAY1)
        next_day = broker.submit(intent(3, Side.SELL, 1000, 10.00))
        assert broker.query(next_day).status.value == "submitted"
        assert broker.query(buy).status.value == "filled"

    def test_odd_lot_position_must_be_sold_in_one_shot(self):
        broker = make_broker()
        # a reference market price first, then an IOC clipped by the
        # displayed depth leaves a 30-share (odd-lot) position
        broker.on_snapshot(snap(1, 0, 9.96, asks=((9.96, 100),)))
        broker.submit(
            intent(
                1, Side.BUY, 100, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER
            )
        )
        broker.on_snapshot(snap(2, 1, 9.95, asks=((9.95, 30),)))
        assert broker.positions()[0].quantity == 30

        broker.on_session_end(DAY1)
        partial_sell = broker.submit(intent(2, Side.SELL, 20, 9.00))
        assert broker.query(partial_sell).status.value == "rejected"

        one_shot = broker.submit(intent(3, Side.SELL, 30, 9.00))
        assert broker.query(one_shot).status.value == "submitted"


# ============================================================================
# Ledger and fees (the shared BacktestAccount + fee schedule)
# ============================================================================
class TestLedgerAndFees:
    def test_buy_books_per_fill_fees(self):
        broker = make_broker()
        recorder = Recorder(broker)
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))

        (fill,) = recorder.fills(order_id)
        # amount 9,950.00: commission max(2.985, 5.00) = 5.00,
        # no stamp duty, transfer 9,950.00 * 0.00001 = 0.10
        assert fill.price == pytest.approx(9.95)
        assert fill.commission == pytest.approx(5.00)
        assert fill.stamp_duty == pytest.approx(0.00)
        assert fill.transfer_fee == pytest.approx(0.10)
        assert broker.cash == pytest.approx(1_000_000.0 - 9_950.0 - 5.10)
        assert broker.positions()[0].available_quantity == 0  # T+1 parked

    def test_sell_round_trip_matches_hand_arithmetic(self):
        broker = make_broker()
        broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))
        broker.on_session_end(DAY1)

        sell = broker.submit(intent(2, Side.SELL, 1000, 10.50))
        broker.on_snapshot(snap(2, 0, 10.55, bids=((10.55, 5000),), day=DAY2))

        assert broker.query(sell).status.value == "filled"
        # amount 10,550.00: commission 5.00, stamp 5.275 -> 5.28 (HALF_UP),
        # transfer 0.1055 -> 0.11; proceeds 10,550.00 - 10.39 = 10,539.61
        assert broker.cash == pytest.approx(990_044.90 + 10_539.61)
        assert broker.positions() == []

    def test_marketable_buy_clipped_to_cash_at_fill_time(self):
        # estimated against the last seen price but filled against a
        # dearer book: the fill clips to what the account can pay
        broker = make_broker(initial_cash=9_905.50)
        broker.on_snapshot(snap(1, 0, 9.90, asks=((9.90, 100),)))
        order_id = broker.submit(
            intent(2, Side.BUY, 1000, None, price_mode=PriceMode.COUNTER_PRICE)
        )

        broker.on_snapshot(snap(2, 1, 9.95, asks=((9.95, 5000),)))

        state = broker.query(order_id)
        assert state.status.value == "partially_filled"
        assert state.filled_quantity == 995  # 1,000 clipped by cash at 9.95
        assert broker.cash >= 0.0


# ============================================================================
# Snapshot semantics (late / duplicated / gaps, symbol isolation)
# ============================================================================
class TestSnapshotSemantics:
    def test_late_snapshot_with_regressed_seq_is_dropped(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(snap(5, 0, 9.95, asks=((9.95, 5000),)))
        assert broker.query(order_id).status.value == "filled"
        filled_at = broker.query(order_id).filled_quantity

        # a late replay of an older book must not double-book anything
        broker.on_snapshot(snap(4, -1, 9.90, asks=((9.90, 5000),)))
        assert broker.dropped_stale_snapshots == 1
        assert broker.query(order_id).filled_quantity == filled_at

    def test_sequence_gaps_are_tolerated(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(snap(3, 0, 9.96, asks=((9.96, 100),)))
        broker.on_snapshot(snap(11, 1, 9.94, asks=((9.94, 5000),)))

        assert broker.query(order_id).status.value == "filled"
        assert broker.dropped_stale_snapshots == 0

    def test_snapshots_only_match_their_own_symbol(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))

        broker.on_snapshot(
            snap(1, 0, 9.95, asks=((9.95, 5000),), symbol="000001")
        )

        assert broker.query(order_id).status.value == "submitted"


# ============================================================================
# Idempotency and queries
# ============================================================================
class TestIdempotencyAndQueries:
    def test_resubmitting_the_same_intent_returns_the_same_order(self):
        broker = make_broker()
        recorder = Recorder(broker)
        first = broker.submit(intent(1))
        again = broker.submit(intent(1))

        assert first == again
        assert recorder.types_of(first) == [ExecutionEventType.ACCEPTED]

    def test_reusing_a_key_for_a_different_intent_conflicts(self):
        broker = make_broker()
        broker.submit(intent(1))
        with pytest.raises(IdempotencyConflictError):
            broker.submit(intent(1, Side.SELL, 500, 10.50))

    def test_cancel_unknown_order(self):
        broker = make_broker()
        result = broker.cancel("ord-nope")
        assert result.accepted is False
        assert result.reason == "unknown order_id"

    def test_cancel_terminal_order(self):
        broker = make_broker()
        order_id = broker.submit(intent(1, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(1, 0, 9.95, asks=((9.95, 5000),)))

        result = broker.cancel(order_id)
        assert result.accepted is False
        assert "already terminal" in (result.reason or "")

    def test_query_unknown_order_raises(self):
        broker = make_broker()
        with pytest.raises(ValueError, match="unknown order_id"):
            broker.query("ord-nope")

    def test_day_order_expires_but_gtc_survives_session_end(self):
        broker = make_broker()
        day_order = broker.submit(intent(1, Side.BUY, 1000, 9.00))
        gtc_order = broker.submit(
            intent(2, Side.BUY, 1000, 9.00, time_in_force=TimeInForce.GTC)
        )

        broker.on_session_end(DAY1)

        assert broker.query(day_order).status.value == "cancelled"
        assert broker.query(gtc_order).status.value == "submitted"


# ============================================================================
# Intraday session smoke (盘中会话冒烟): simulated snapshot clock, no sleeps
# ============================================================================
class TestIntradaySessionSmoke:
    def test_full_session_with_archive(self, tmp_path: Path):
        archive = EventArchive(tmp_path / "run-e3")
        broker = PaperBroker(
            matching=MatchingRules(strict_price_boundary=True),
            instruments=[MAIN_INSTRUMENT],
            clock=ts(-20),
            archive=archive,
        )
        recorder = Recorder(broker)
        assert broker.state is PaperSessionState.IDLE

        # pre-open intent is refused
        refused = broker.submit(intent(1))
        assert broker.query(refused).status.value == "rejected"

        broker.start(now=ts(-15))  # 09:15, ahead of the continuous auction
        assert broker.state is PaperSessionState.RUNNING

        # 09:30 continuous auction opens: a book rests below the 10.00 limit
        buy = broker.submit(intent(2, Side.BUY, 1000, 10.00))
        broker.on_snapshot(snap(100, 0, 9.95, asks=((9.95, 5000),)))
        assert broker.query(buy).status.value == "filled"

        # 10:00: a same-day sell hits the T+1 wall
        sell_same_day = broker.submit(intent(3, Side.SELL, 1000, 10.00))
        assert broker.query(sell_same_day).status.value == "rejected"

        # 10:30: a resting order is cancelled manually
        resting = broker.submit(intent(4, Side.BUY, 1000, 9.00))
        broker.on_snapshot(snap(101, 60, 9.90, asks=((9.90, 8000),)))
        assert broker.query(resting).status.value == "submitted"
        assert broker.cancel(resting).accepted is True
        assert broker.query(resting).status.value == "cancelled"

        # 11:30 (morning close): five-level IOC takes what the book shows
        ioc = broker.submit(
            intent(
                5, Side.BUY, 1000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER
            )
        )
        broker.on_snapshot(snap(103, 120, 9.88, asks=((9.88, 250), (9.89, 150))))
        assert broker.query(ioc).filled_quantity == 400
        assert broker.query(ioc).status.value == "cancelled"

        # a late replay of the morning (regressed seq) is dropped silently
        broker.on_snapshot(snap(102, 119, 9.10, asks=((9.10, 999_999),)))
        assert broker.dropped_stale_snapshots == 1

        # 14:59 (afternoon): the day's last snapshot fixes the clock
        broker.on_snapshot(snap(104, 359, 9.92, asks=((9.92, 3000),)))
        assert broker.clock == ts(359).replace(tzinfo=SHANGHAI_TZ)

        # 15:00 session close: DAY orders expire, T+1 rolls, close fixed
        broker.on_session_end(DAY1)
        assert broker.positions()[0].quantity == 1400
        assert broker.positions()[0].available_quantity == 1400

        # stop: audit trail closes
        broker.stop("smoke complete")
        assert broker.state is PaperSessionState.STOPPED
        assert [record.kind for record in broker.session_trail()] == [
            "started",
            "session_end",
            "stopped",
        ]

        # every emitted event landed in the JSONL archive, in order
        archived = archive.read_lines()
        assert len(archived) == len(recorder.events)
        assert [row["event_type"] for row in archived] == [
            event.event_type.value for event in recorder.events
        ]

        # the ledger reconciles: cash moved only through booked fills
        fills = [event.fill for event in recorder.events if event.fill is not None]
        spent = round(
            sum(
                fill.price * fill.quantity
                + fill.commission
                + fill.stamp_duty
                + fill.transfer_fee
                for fill in fills
            ),
            2,
        )
        assert broker.cash == pytest.approx(1_000_000.0 - spent)
