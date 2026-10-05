"""BacktestVenue golden and behavioural tests.

The three golden acceptance cases of the Pulsar execution design run
first, each with the full manual derivation in comments:

1. 涨跌停一字板不成交 — sealed one-line limit boards never fill;
2. T+1 当日买入不可卖 — same-day sells of today's buys are rejected;
3. 费用逐笔计算与手工算例一致 — per-fill fees match hand arithmetic.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pulsar_contracts import (
    Bar,
    Board,
    ExecutionEvent,
    ExecutionEventType,
    ExecutionPort,
    Exchange,
    Freq,
    IdempotencyKey,
    Instrument,
    InstrumentStatus,
    OrderId,
    OrderIntent,
    OrderStatus,
    PriceMode,
    Side,
    TimeInForce,
)

from pulsar_exec import BacktestVenue, MatchingRules, SlippageModel

DAY1 = date(2026, 10, 5)
DAY2 = date(2026, 10, 6)

MAIN_INSTRUMENT = Instrument(
    symbol="600519",
    exchange=Exchange.SSE,
    board=Board.MAIN,
    list_date=date(2001, 8, 27),
)


def make_bar(
    day: date,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    symbol: str = "600519",
    freq: Freq = Freq.DAILY,
    minute: int = 0,
) -> Bar:
    return Bar(
        symbol=symbol,
        ts=datetime(day.year, day.month, day.day, minute // 60, minute % 60),
        freq=freq,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=close * volume,
    )


def make_venue(**overrides) -> BacktestVenue:
    """Venue with exact-arithmetic defaults (no slippage penalty)."""
    defaults: dict[str, object] = {
        "initial_cash": 1_000_000.0,
        "slippage": SlippageModel(fixed_bps=0.0),
        "instruments": [MAIN_INSTRUMENT],
        "clock": datetime(2026, 10, 5, 9, 30),
    }
    defaults.update(overrides)
    return BacktestVenue(**defaults)  # type: ignore[arg-type]


def intent(
    seq: int,
    side: Side = Side.BUY,
    quantity: int = 1000,
    limit_price: float | None = 10.00,
    symbol: str = "600519",
    price_mode: PriceMode = PriceMode.LIMIT,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> OrderIntent:
    return OrderIntent(
        idempotency_key=IdempotencyKey(run_id="run-e2", seq=seq),
        side=side,
        symbol=symbol,
        quantity=quantity,
        price_mode=price_mode,
        limit_price=limit_price,
        time_in_force=time_in_force,
    )


class Recorder:
    """Collects every execution event pushed by the venue."""

    def __init__(self, venue: BacktestVenue) -> None:
        self.events: list[ExecutionEvent] = []
        venue.on_event(self.events.append)

    def of(self, order_id: OrderId) -> list[ExecutionEvent]:
        return [event for event in self.events if event.order_id == order_id]

    def types_of(self, order_id: OrderId) -> list[ExecutionEventType]:
        return [event.event_type for event in self.of(order_id)]

    def fills(self, order_id: OrderId):
        return [
            event.fill
            for event in self.of(order_id)
            if event.event_type
            in (ExecutionEventType.PARTIAL_FILL, ExecutionEventType.FILL)
        ]


def drive_round_trip_setup(venue: BacktestVenue) -> OrderId:
    """Day 1: buy 1000 @ 10.00 against a normal bar; close the day.

    Leaves: cash 989,994.90 ; position 1000 @ avg 10.0051 (all T+1 parked);
    prev close fixed at 10.00 (day-2 band [9.00, 11.00]).
    """
    order_id = venue.submit(intent(1, Side.BUY, 1000, 10.00))
    venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))
    venue.on_session_end(DAY1)
    return order_id


# ============================================================================
# Golden case 1: 涨跌停一字板不成交
# ============================================================================
class TestGoldenOneLineBoard:
    def test_buy_at_sealed_limit_up_never_fills(self):
        venue = make_venue()
        drive_round_trip_setup(venue)

        # Day 2: limit-up one-line board. prev close 10.00 -> limit up
        # 11.00; bar locked O=H=L=C=11.00 all day: no counterparty ever
        # offered below the sealed price, so a buy CANNOT fill.
        buy = venue.submit(intent(2, Side.BUY, 1000, 11.00))
        venue.on_bar(make_bar(DAY2, 11.00, 11.00, 11.00, 11.00, 50_000))

        assert venue.query(buy).status is OrderStatus.SUBMITTED
        assert venue.query(buy).filled_quantity == 0

        # the day closes; the DAY order expires unfilled
        venue.on_session_end(DAY2)
        assert venue.query(buy).status is OrderStatus.CANCELLED

    def test_sell_into_sealed_limit_down_never_fills(self):
        venue = make_venue()
        drive_round_trip_setup(venue)  # holds 1000 sellable shares

        # Day 2: limit-down one-line board at 9.00 — no buyer ever bid at
        # or above the sealed price, so a sell CANNOT fill.
        sell = venue.submit(intent(2, Side.SELL, 1000, 9.00))
        venue.on_bar(make_bar(DAY2, 9.00, 9.00, 9.00, 9.00, 50_000))

        assert venue.query(sell).status is OrderStatus.SUBMITTED
        assert venue.query(sell).filled_quantity == 0
        assert venue.positions()[0].quantity == 1000  # nothing moved

        venue.on_session_end(DAY2)
        assert venue.query(sell).status is OrderStatus.CANCELLED

    def test_limit_board_with_range_still_fills(self):
        """Only the *sealed* (一字) board blocks fills, not any limit touch."""
        venue = make_venue()
        drive_round_trip_setup(venue)

        buy = venue.submit(intent(2, Side.BUY, 1000, 11.00))
        # touched limit up intraday but traded a range: fillable
        venue.on_bar(make_bar(DAY2, 10.50, 11.00, 10.40, 11.00, 200_000))

        assert venue.query(buy).status is OrderStatus.FILLED
        assert venue.query(buy).filled_quantity == 1000


# ============================================================================
# Golden case 2: T+1 当日买入不可卖
# ============================================================================
class TestGoldenTPlusOne:
    def test_same_day_sell_of_todays_buy_is_rejected(self):
        venue = make_venue()
        recorder = Recorder(venue)

        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))

        # bought today -> available 0 although quantity is 1000
        position = venue.positions()[0]
        assert position.quantity == 1000
        assert position.available_quantity == 0

        # same-day sell attempt: REJECTED by the T+1 constraint
        sell = venue.submit(intent(2, Side.SELL, 1000, 10.50))
        assert venue.query(sell).status is OrderStatus.REJECTED
        reason = recorder.of(sell)[0].reason
        assert reason is not None and "T+1 available" in reason
        assert recorder.types_of(sell) == [ExecutionEventType.REJECTED]

        # the day rolls: the same holding becomes sellable
        venue.on_session_end(DAY1)
        assert venue.positions()[0].available_quantity == 1000
        assert recorder.types_of(buy) == [ExecutionEventType.ACCEPTED, ExecutionEventType.FILL]

    def test_sell_fills_next_day_after_roll(self):
        venue = make_venue()
        drive_round_trip_setup(venue)

        sell = venue.submit(intent(2, Side.SELL, 1000, 10.50))
        venue.on_bar(make_bar(DAY2, 10.10, 10.60, 9.95, 10.50, 1_000_000))

        assert venue.query(sell).status is OrderStatus.FILLED
        assert venue.positions() == []  # flat again


# ============================================================================
# Golden case 3: 费用逐笔计算与手工算例一致
# ============================================================================
class TestGoldenFeePerFill:
    def test_round_trip_fees_match_hand_arithmetic(self):
        venue = make_venue()
        recorder = Recorder(venue)

        # ---- Day 1: BUY 1000 @ 10.00 --------------------------------------
        # base fill price = min(open=10.00, limit=10.00) = 10.00 (slippage 0)
        #   amount        = 10.00 * 1000 = 10,000.00 CNY
        #   commission    = max(10,000.00 * 0.0003, 5.00) = max(3.00, 5.00) = 5.00
        #   stamp duty    = 0.00                     (buys pay none)
        #   transfer fee  = 10,000.00 * 0.00001 = 0.105 -> wait, exactly
        #                   10,000 * 0.00001 = 0.10    (no rounding needed)
        #   total fees    = 5.10
        #   cash          = 1,000,000.00 - (10,000.00 + 5.10) = 989,994.90
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))

        (buy_fill,) = recorder.fills(buy)
        assert buy_fill.price == pytest.approx(10.00)
        assert buy_fill.quantity == 1000
        assert buy_fill.commission == pytest.approx(5.00)
        assert buy_fill.stamp_duty == pytest.approx(0.00)
        assert buy_fill.transfer_fee == pytest.approx(0.10)
        assert venue.cash == pytest.approx(989_994.90)

        venue.on_session_end(DAY1)

        # ---- Day 2: SELL 1000 @ 10.50 -------------------------------------
        # base fill price = max(open=10.10, limit=10.50) = 10.50 (the bar
        #                  never traded above the limit, so the limit binds)
        #   amount        = 10.50 * 1000 = 10,500.00 CNY
        #   commission    = max(10,500.00 * 0.0003, 5.00) = max(3.15, 5.00) = 5.00
        #   stamp duty    = 10,500.00 * 0.0005 = 5.25    (sells only)
        #   transfer fee  = 10,500.00 * 0.00001 = 0.105 -> 0.11 (HALF_UP)
        #   total fees    = 10.36
        #   proceeds      = 10,500.00 - 10.36 = 10,489.64
        #   cash          = 989,994.90 + 10,489.64 = 1,000,484.54
        sell = venue.submit(intent(2, Side.SELL, 1000, 10.50))
        venue.on_bar(make_bar(DAY2, 10.10, 10.60, 9.95, 10.50, 1_000_000))

        (sell_fill,) = recorder.fills(sell)
        assert sell_fill.price == pytest.approx(10.50)
        assert sell_fill.quantity == 1000
        assert sell_fill.commission == pytest.approx(5.00)
        assert sell_fill.stamp_duty == pytest.approx(5.25)
        assert sell_fill.transfer_fee == pytest.approx(0.11)
        assert venue.cash == pytest.approx(1_000_484.54)

        # average fill price of the sell order mirrors the fill
        assert venue.query(sell).avg_fill_price == pytest.approx(10.50)


# ============================================================================
# Fillability mechanics
# ============================================================================
class TestVolumeParticipation:
    def test_order_capped_at_fraction_of_bar_volume(self):
        venue = make_venue()
        recorder = Recorder(venue)

        # bar volume 5,000 -> participation cap 10% = 500 shares per bar
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.10, 9.95, 10.05, 5_000))
        assert venue.query(buy).status is OrderStatus.PARTIALLY_FILLED
        assert venue.query(buy).filled_quantity == 500

        # next bar (volume 8,000 -> cap 800) fills the remaining 500
        venue.on_bar(make_bar(DAY1, 10.02, 10.08, 9.98, 10.04, 8_000, minute=1435))
        assert venue.query(buy).status is OrderStatus.FILLED
        assert venue.query(buy).filled_quantity == 1000
        assert recorder.types_of(buy) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.PARTIAL_FILL,
            ExecutionEventType.FILL,
        ]

    def test_cap_is_shared_across_orders_fifo(self):
        venue = make_venue()
        first = venue.submit(intent(1, Side.BUY, 500, 10.00))
        second = venue.submit(intent(2, Side.BUY, 500, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.10, 9.95, 10.05, 5_000))

        # cap 500 consumed entirely by the first (FIFO) order
        assert venue.query(first).status is OrderStatus.FILLED
        assert venue.query(second).status is OrderStatus.SUBMITTED
        assert venue.query(second).filled_quantity == 0

    def test_partial_fill_fees_are_per_fill(self):
        venue = make_venue()
        recorder = Recorder(venue)

        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.10, 9.95, 10.05, 5_000))
        # partial fill of 500 @ 10.00:
        #   commission = max(5,000.00 * 0.0003 = 1.50, 5.00) = 5.00
        #   transfer   = 5,000.00 * 0.00001 = 0.05
        (partial,) = recorder.fills(buy)
        assert partial.quantity == 500
        assert partial.commission == pytest.approx(5.00)
        assert partial.transfer_fee == pytest.approx(0.05)


class TestStrictBoundaryMode:
    def test_strict_mode_requires_penetration(self):
        venue = make_venue(matching=MatchingRules(strict_price_boundary=True))
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        # bar low exactly equal to the limit: touch, not penetration
        venue.on_bar(make_bar(DAY1, 10.05, 10.20, 10.00, 10.10, 1_000_000))
        assert venue.query(buy).filled_quantity == 0

    def test_default_mode_fills_on_touch(self):
        venue = make_venue()
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.on_bar(make_bar(DAY1, 10.05, 10.20, 10.00, 10.10, 1_000_000))
        assert venue.query(buy).status is OrderStatus.FILLED
        # fill at the limit (open was above it)
        assert venue.query(buy).avg_fill_price == pytest.approx(10.00)

    def test_strict_mode_sell_symmetry(self):
        venue = make_venue(matching=MatchingRules(strict_price_boundary=True))
        drive_round_trip_setup(venue)
        sell = venue.submit(intent(2, Side.SELL, 1000, 10.20))
        # bar high exactly equal to the limit: touch, not penetration
        venue.on_bar(make_bar(DAY2, 10.10, 10.20, 9.95, 10.10, 1_000_000))
        assert venue.query(sell).filled_quantity == 0


class TestGapOpenFillsAtBetterPrice:
    def test_buy_gap_down_fills_at_open(self):
        venue = make_venue()
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        # opens at 9.80, below the limit: fills at the better open
        venue.on_bar(make_bar(DAY1, 9.80, 9.95, 9.70, 9.90, 1_000_000))
        assert venue.query(buy).avg_fill_price == pytest.approx(9.80)

    def test_sell_gap_up_fills_at_open(self):
        venue = make_venue()
        drive_round_trip_setup(venue)
        sell = venue.submit(intent(2, Side.SELL, 1000, 10.50))
        venue.on_bar(make_bar(DAY2, 10.70, 10.90, 10.60, 10.80, 1_000_000))
        assert venue.query(sell).avg_fill_price == pytest.approx(10.70)


# ============================================================================
# Suspension (停牌)
# ============================================================================
class TestSuspension:
    def test_marked_suspended_symbol_never_fills(self):
        venue = make_venue()
        buy = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        venue.mark_suspended("600519", DAY1)
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))

        assert venue.query(buy).filled_quantity == 0
        assert venue.query(buy).status is OrderStatus.SUBMITTED
        assert venue.is_suspended("600519", DAY1)

    def test_suspended_instrument_rejects_submission(self):
        instrument = MAIN_INSTRUMENT.model_copy(
            update={"status": InstrumentStatus.SUSPENDED}
        )
        venue = make_venue(instruments=[instrument])
        recorder = Recorder(venue)

        order = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        assert venue.query(order).status is OrderStatus.REJECTED
        reason = recorder.of(order)[0].reason
        assert reason is not None and "停牌" in reason


# ============================================================================
# Venue-side pre-trade validation
# ============================================================================
class TestPreTradeValidation:
    def test_buy_must_be_board_lot(self):
        venue = make_venue()
        order = venue.submit(intent(1, Side.BUY, 150, 10.00))
        assert venue.query(order).status is OrderStatus.REJECTED

    def test_insufficient_funds_rejected(self):
        venue = make_venue(initial_cash=5_000.0)
        # estimate 10.00 * 1000 + 5.10 fees = 10,005.10 > 5,000.00 cash
        order = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        assert venue.query(order).status is OrderStatus.REJECTED

    def test_limit_price_outside_band_rejected(self):
        venue = make_venue()
        drive_round_trip_setup(venue)  # prev close 10.00 -> band [9.00, 11.00]

        too_high = venue.submit(intent(2, Side.BUY, 1000, 11.50))
        too_low = venue.submit(intent(3, Side.SELL, 1000, 8.99))
        boundary = venue.submit(intent(4, Side.BUY, 1000, 11.00))

        assert venue.query(too_high).status is OrderStatus.REJECTED
        assert venue.query(too_low).status is OrderStatus.REJECTED
        assert venue.query(boundary).status is OrderStatus.SUBMITTED

    def test_st_band_is_five_percent(self):
        st = MAIN_INSTRUMENT.model_copy(update={"is_st": True})
        venue = make_venue(instruments=[st])
        drive_round_trip_setup(venue)  # prev close 10.00 -> ST band [9.50, 10.50]

        above = venue.submit(intent(2, Side.BUY, 1000, 10.80))
        assert venue.query(above).status is OrderStatus.REJECTED

    def test_odd_lot_position_must_be_sold_in_one_shot(self):
        venue = make_venue()
        # bar volume 5,500 -> cap 550: a 600-share order fills 550 (odd)
        buy = venue.submit(intent(1, Side.BUY, 600, 10.00))
        venue.on_bar(make_bar(DAY1, 10.00, 10.10, 9.95, 10.05, 5_500))
        assert venue.query(buy).filled_quantity == 550
        venue.on_session_end(DAY1)  # 550 odd shares now available

        partial_sell = venue.submit(intent(2, Side.SELL, 500, 10.50))
        assert venue.query(partial_sell).status is OrderStatus.REJECTED

        full_sell = venue.submit(intent(3, Side.SELL, 550, 10.50))
        venue.on_bar(make_bar(DAY2, 10.10, 10.60, 9.95, 10.50, 1_000_000))
        assert venue.query(full_sell).status is OrderStatus.FILLED
        assert venue.positions() == []

    def test_marketable_buy_without_reference_price_rejected(self):
        venue = make_venue()
        order = venue.submit(intent(1, Side.BUY, 1000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER))
        assert venue.query(order).status is OrderStatus.REJECTED


# ============================================================================
# Fill-time cash clipping
# ============================================================================
class TestAffordabilityClipping:
    def test_slipped_fill_clips_to_affordable_quantity(self):
        # cash 12,010.00; buy 2000 @ limit 6.00 (estimate 12,005.10 fits).
        # Slippage 10 bps lifts the fill price to 6.00 * 1.001 = 6.006 ->
        # 6.01, so the full 2000 shares (12,020 + fees) no longer fit:
        #   1999 @ 6.01 = 12,013.99 + 5.12 = 12,019.11 > 12,010  (no)
        #   1998 @ 6.01 = 12,007.98 + 5.12 = 12,013.10 > 12,010  (no)
        #   1997 @ 6.01 = 12,001.97 + 5.12 = 12,007.09 <= 12,010 (yes)
        # -> PARTIAL_FILL of 1997 shares; cash 12,010.00 - 12,007.09 = 2.91
        venue = make_venue(
            initial_cash=12_010.0, slippage=SlippageModel(fixed_bps=10.0)
        )
        buy = venue.submit(intent(1, Side.BUY, 2000, 6.00, symbol="600000"))
        venue.on_bar(
            make_bar(DAY1, 6.00, 6.05, 5.90, 6.00, 100_000, symbol="600000")
        )

        state = venue.query(buy)
        assert state.status is OrderStatus.PARTIALLY_FILLED
        assert state.filled_quantity == 1997
        assert state.avg_fill_price == pytest.approx(6.01)
        assert venue.cash == pytest.approx(2.91)


# ============================================================================
# Idempotency and the port contract
# ============================================================================
class TestIdempotency:
    def test_resubmission_returns_same_order_without_duplicate_events(self):
        venue = make_venue()
        recorder = Recorder(venue)

        first = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        second = venue.submit(intent(1, Side.BUY, 1000, 10.00))  # retry

        assert second == first
        assert recorder.types_of(first) == [ExecutionEventType.ACCEPTED]

    def test_different_keys_yield_different_orders(self):
        venue = make_venue()
        first = venue.submit(intent(1, Side.BUY, 1000, 10.00))
        second = venue.submit(intent(2, Side.BUY, 1000, 10.00))
        assert first != second


class TestPortContract:
    def test_venue_implements_execution_port(self):
        assert isinstance(make_venue(), ExecutionPort)

    def test_query_unknown_order_raises(self):
        with pytest.raises(ValueError, match="unknown order_id"):
            make_venue().query(OrderId("ord-nope"))


# ============================================================================
# Marketable intents (counter price / five-level IOC)
# ============================================================================
class TestMarketableIntents:
    def test_counter_price_fills_at_close_across_bars(self):
        venue = make_venue()
        # counter-price needs a reference for the funds check: prime with
        # a first session, then submit on day 2
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))
        venue.on_session_end(DAY1)
        buy = venue.submit(
            intent(1, Side.BUY, 1000, None, price_mode=PriceMode.COUNTER_PRICE)
        )

        venue.on_bar(make_bar(DAY2, 10.00, 10.10, 9.95, 10.05, 4_000))
        assert venue.query(buy).filled_quantity == 400  # cap 10% of 4,000
        venue.on_bar(make_bar(DAY2, 10.10, 10.20, 10.00, 10.10, 100_000, minute=1435))
        assert venue.query(buy).status is OrderStatus.FILLED
        assert venue.query(buy).avg_fill_price == pytest.approx(
            (400 * 10.05 + 600 * 10.10) / 1000
        )

    def test_five_level_ioc_cancels_remainder_same_bar(self):
        venue = make_venue()
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))
        venue.on_session_end(DAY1)
        recorder = Recorder(venue)

        buy = venue.submit(
            intent(1, Side.BUY, 2000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER)
        )
        venue.on_bar(make_bar(DAY2, 10.10, 10.30, 10.00, 10.20, 5_000))

        assert recorder.types_of(buy) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.PARTIAL_FILL,
            ExecutionEventType.CANCELLED,
        ]
        state = venue.query(buy)
        assert state.status is OrderStatus.CANCELLED
        assert state.filled_quantity == 500  # cap 10% of 5,000

    def test_five_level_ioc_with_zero_volume_cancels_without_fill(self):
        venue = make_venue()
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))
        venue.on_session_end(DAY1)

        buy = venue.submit(
            intent(1, Side.BUY, 2000, None, price_mode=PriceMode.FIVE_LEVEL_CANCEL_REMAINDER)
        )
        venue.on_bar(make_bar(DAY2, 10.00, 10.00, 10.00, 10.00, 0))
        assert venue.query(buy).status is OrderStatus.CANCELLED
        assert venue.query(buy).filled_quantity == 0


# ============================================================================
# Day-end lifecycle
# ============================================================================
class TestSessionLifecycle:
    def test_day_orders_expire_at_session_close(self):
        venue = make_venue()
        recorder = Recorder(venue)
        buy = venue.submit(intent(1, Side.BUY, 1000, 9.50))
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.60, 10.00, 1_000_000))

        venue.on_session_end(DAY1)
        assert venue.query(buy).status is OrderStatus.CANCELLED
        assert recorder.types_of(buy) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.CANCELLED,
        ]

    def test_gtc_orders_survive_session_close(self):
        venue = make_venue()
        recorder = Recorder(venue)
        buy = venue.submit(intent(1, Side.BUY, 1000, 9.50, time_in_force=TimeInForce.GTC))
        venue.on_bar(make_bar(DAY1, 10.00, 10.20, 9.60, 10.00, 1_000_000))
        venue.on_session_end(DAY1)
        assert venue.query(buy).status is OrderStatus.SUBMITTED

        venue.on_bar(make_bar(DAY2, 9.60, 9.90, 9.40, 9.50, 1_000_000))
        assert venue.query(buy).status is OrderStatus.FILLED
        assert recorder.types_of(buy) == [
            ExecutionEventType.ACCEPTED,
            ExecutionEventType.FILL,
        ]

    def test_cancel_api(self):
        venue = make_venue()
        buy = venue.submit(intent(1, Side.BUY, 1000, 9.50))

        result = venue.cancel(buy)
        assert result.accepted is True
        assert result.status is OrderStatus.CANCELLED
        assert venue.query(buy).status is OrderStatus.CANCELLED

        again = venue.cancel(buy)
        assert again.accepted is False
        assert again.status is OrderStatus.CANCELLED

    def test_cancel_unknown_order(self):
        venue = make_venue()
        result = venue.cancel(OrderId("ord-nope"))
        assert result.accepted is False
        assert result.reason == "unknown order_id"


class TestDefaultSlippageVenue:
    def test_default_five_bps_applies_to_fills(self):
        # venue default slippage: 5 bps. Buy limit 100.00 fills at the
        # limit; slipped price = 100.00 * 1.0005 = 100.05.
        venue = BacktestVenue(
            initial_cash=1_000_000.0,
            instruments=[
                Instrument(
                    symbol="600900",
                    exchange=Exchange.SSE,
                    board=Board.MAIN,
                    list_date=date(2000, 1, 1),
                )
            ],
            clock=datetime(2026, 10, 5, 9, 30),
        )
        buy = venue.submit(intent(1, Side.BUY, 100, 100.00, symbol="600900"))
        venue.on_bar(
            make_bar(DAY1, 100.00, 100.50, 99.90, 100.20, 1_000_000, symbol="600900")
        )
        assert venue.query(buy).avg_fill_price == pytest.approx(100.05)
