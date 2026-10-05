"""Channel-consistency acceptance test: BacktestVenue vs PaperBroker.

The Pulsar execution design's acceptance criterion for the paper
channel: *the same sample strategy produces identical order-state
transition semantics on the backtest venue (bar replay) and the paper
broker (snapshot driving)* — the same intent sequence, submitted at the
same logical points of the same market progression, must yield the same
event-type (hence state-transition) sequence on both channels.

The script below runs one identical intent sequence through two trading
days on both channels: a full fill, a T+1 rejection, a next-day sell, a
partial-then-complete fill constrained by market depth, a manual cancel,
a five-level IOC partial with immediate remainder cancel, and a DAY
order expiring at session close. Prices and market-data shapes differ
(bars vs five-level books); the *lifecycle* must not.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from pulsar_contracts import (
    Bar,
    Board,
    Exchange,
    ExecutionEvent,
    ExecutionEventType,
    ExecutionPort,
    Freq,
    IdempotencyKey,
    Instrument,
    OrderId,
    OrderIntent,
    PriceMode,
    QuoteLevel,
    Side,
    Snapshot,
    TimeInForce,
)

from pulsar_exec import BacktestVenue, MatchingRules, PaperBroker, SlippageModel

DAY1 = date(2026, 10, 5)
DAY2 = date(2026, 10, 6)

INSTRUMENT = Instrument(
    symbol="600519",
    exchange=Exchange.SSE,
    board=Board.MAIN,
    list_date=date(2001, 8, 27),
)


def intent(seq: int, side: Side, quantity: int, limit_price: float | None) -> OrderIntent:
    """The identical intent factory handed to both channels."""
    price_mode = (
        PriceMode.LIMIT
        if limit_price is not None
        else PriceMode.FIVE_LEVEL_CANCEL_REMAINDER
    )
    return OrderIntent(
        idempotency_key=IdempotencyKey(run_id="run-consistency", seq=seq),
        side=side,
        symbol="600519",
        quantity=quantity,
        price_mode=price_mode,
        limit_price=limit_price,
        time_in_force=TimeInForce.DAY,
    )


class Channel:
    """One execution channel driven through its own market-data shape."""

    def __init__(self, name: str, port: ExecutionPort) -> None:
        self.name = name
        self.port = port
        self.events: list[ExecutionEvent] = []
        port.on_event(self.events.append)
        self.order_ids: dict[str, OrderId] = {}

    def submit(self, label: str, order_intent: OrderIntent) -> None:
        self.order_ids[label] = self.port.submit(order_intent)

    def types_of(self, label: str) -> list[ExecutionEventType]:
        order_id = self.order_ids[label]
        return [event.event_type for event in self.events if event.order_id == order_id]


def bar(
    day: date,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: float,
    *,
    minute: int = 0,
) -> Bar:
    return Bar(
        symbol="600519",
        ts=datetime(day.year, day.month, day.day, 9, 30) + timedelta(minutes=minute),
        freq=Freq.MINUTE,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=close * volume,
    )


def snapshot(seq: int, day: date, last: float, *, asks=(), bids=()) -> Snapshot:
    return Snapshot(
        symbol="600519",
        ts=datetime(day.year, day.month, day.day, 9, 30) + timedelta(minutes=seq),
        seq=seq,
        last_price=last,
        volume=1_000_000.0,
        amount=last * 1_000_000.0,
        bids=tuple(QuoteLevel(price=p, volume=v) for p, v in bids),
        asks=tuple(QuoteLevel(price=p, volume=v) for p, v in asks),
    )


def run_backtest_channel() -> Channel:
    venue = BacktestVenue(
        initial_cash=1_000_000.0,
        slippage=SlippageModel(fixed_bps=0.0),  # exact arithmetic
        instruments=[INSTRUMENT],
        clock=datetime(2026, 10, 5, 9, 30),
    )
    channel = Channel("backtest", venue)

    # ---- Day 1 ----------------------------------------------------------
    channel.submit("full_fill_buy", intent(1, Side.BUY, 1000, 10.00))
    venue.on_bar(bar(DAY1, 10.00, 10.20, 9.90, 10.00, 1_000_000))  # low penetrates
    channel.submit("t_plus_one_rejected_sell", intent(2, Side.SELL, 1000, 10.50))
    venue.on_session_end(DAY1)  # T+1 rolls, prev close fixed

    # ---- Day 2 ----------------------------------------------------------
    channel.submit("next_day_sell", intent(3, Side.SELL, 1000, 10.10))
    venue.on_bar(bar(DAY2, 10.02, 10.15, 10.01, 10.12, 1_000_000, minute=0))  # high penetrates
    channel.submit("partial_then_complete_buy", intent(4, Side.BUY, 2000, 10.20))
    venue.on_bar(bar(DAY2, 10.12, 10.25, 10.11, 10.18, 4_000, minute=30))  # 10% cap = 400
    venue.on_bar(bar(DAY2, 10.15, 10.22, 10.09, 10.15, 1_000_000, minute=60))  # completes
    channel.submit("manual_cancel_buy", intent(5, Side.BUY, 1000, 9.50))
    venue.on_bar(bar(DAY2, 10.10, 10.18, 9.90, 10.00, 500_000, minute=90))  # rests
    assert venue.cancel(channel.order_ids["manual_cancel_buy"]).accepted
    channel.submit("five_level_ioc_buy", intent(6, Side.BUY, 1000, None))
    venue.on_bar(bar(DAY2, 10.00, 10.10, 9.95, 10.05, 3_000, minute=120))  # 10% cap = 300
    channel.submit("day_expiry_buy", intent(7, Side.BUY, 1000, 9.40))
    venue.on_bar(bar(DAY2, 10.05, 10.10, 9.95, 10.02, 800_000, minute=150))  # rests
    venue.on_session_end(DAY2)  # DAY order expires
    return channel


def run_paper_channel() -> Channel:
    broker = PaperBroker(
        initial_cash=1_000_000.0,
        matching=MatchingRules(strict_price_boundary=True),
        instruments=[INSTRUMENT],
        clock=datetime(2026, 10, 5, 9, 25),
    )
    channel = Channel("paper", broker)
    broker.start(now=datetime(2026, 10, 5, 9, 25))

    # ---- Day 1 (the snapshot seq carries on monotonically) --------------
    channel.submit("full_fill_buy", intent(1, Side.BUY, 1000, 10.00))
    broker.on_snapshot(snapshot(1, DAY1, 9.95, asks=((9.95, 5000),)))  # penetrates
    channel.submit("t_plus_one_rejected_sell", intent(2, Side.SELL, 1000, 10.50))
    broker.on_session_end(DAY1)  # T+1 rolls, prev close fixed

    # ---- Day 2 ----------------------------------------------------------
    channel.submit("next_day_sell", intent(3, Side.SELL, 1000, 10.10))
    broker.on_snapshot(snapshot(2, DAY2, 10.12, asks=((10.13, 2000),), bids=((10.12, 6000),)))
    channel.submit("partial_then_complete_buy", intent(4, Side.BUY, 2000, 10.20))
    broker.on_snapshot(snapshot(3, DAY2, 10.15, asks=((10.15, 400), (10.21, 9000))))
    broker.on_snapshot(snapshot(4, DAY2, 10.15, asks=((10.15, 5000),)))
    channel.submit("manual_cancel_buy", intent(5, Side.BUY, 1000, 9.50))
    broker.on_snapshot(snapshot(5, DAY2, 10.00, asks=((10.02, 3000),)))  # rests
    assert broker.cancel(channel.order_ids["manual_cancel_buy"]).accepted
    channel.submit("five_level_ioc_buy", intent(6, Side.BUY, 1000, None))
    broker.on_snapshot(snapshot(6, DAY2, 10.05, asks=((10.04, 300),)))
    channel.submit("day_expiry_buy", intent(7, Side.BUY, 1000, 9.40))
    broker.on_snapshot(snapshot(7, DAY2, 10.02, asks=((10.01, 2000),)))  # rests
    broker.on_session_end(DAY2)  # DAY order expires
    broker.stop("scenario complete")
    return channel


LABELS = [
    "full_fill_buy",
    "t_plus_one_rejected_sell",
    "next_day_sell",
    "partial_then_complete_buy",
    "manual_cancel_buy",
    "five_level_ioc_buy",
    "day_expiry_buy",
]


class TestChannelConsistency:
    def test_identical_intent_sequence_yields_identical_event_sequences(self):
        backtest = run_backtest_channel()
        paper = run_paper_channel()

        for label in LABELS:
            assert backtest.types_of(label) == paper.types_of(label), (
                f"event sequences diverge for {label}: "
                f"{[e.value for e in backtest.types_of(label)]} (backtest) vs "
                f"{[e.value for e in paper.types_of(label)]} (paper)"
            )

    def test_final_order_states_match(self):
        backtest = run_backtest_channel()
        paper = run_paper_channel()

        for label in LABELS:
            left = backtest.port.query(backtest.order_ids[label])
            right = paper.port.query(paper.order_ids[label])
            assert left.status is right.status, f"status diverges for {label}"
            assert left.filled_quantity == right.filled_quantity, (
                f"filled quantity diverges for {label}: "
                f"{left.filled_quantity} vs {right.filled_quantity}"
            )

    def test_final_positions_match(self):
        backtest = run_backtest_channel()
        paper = run_paper_channel()

        def book(channel: Channel):
            return [
                (p.symbol, p.quantity, p.available_quantity)
                for p in channel.port.positions()
            ]

        assert book(backtest) == book(paper)

    def test_expected_terminal_states(self):
        """The scenario resolves to these lifecycles on both channels."""
        expected = {
            "full_fill_buy": [ExecutionEventType.ACCEPTED, ExecutionEventType.FILL],
            "t_plus_one_rejected_sell": [ExecutionEventType.REJECTED],
            "next_day_sell": [ExecutionEventType.ACCEPTED, ExecutionEventType.FILL],
            "partial_then_complete_buy": [
                ExecutionEventType.ACCEPTED,
                ExecutionEventType.PARTIAL_FILL,
                ExecutionEventType.FILL,
            ],
            "manual_cancel_buy": [
                ExecutionEventType.ACCEPTED,
                ExecutionEventType.CANCELLED,
            ],
            "five_level_ioc_buy": [
                ExecutionEventType.ACCEPTED,
                ExecutionEventType.PARTIAL_FILL,
                ExecutionEventType.CANCELLED,
            ],
            "day_expiry_buy": [
                ExecutionEventType.ACCEPTED,
                ExecutionEventType.CANCELLED,
            ],
        }
        for channel in (run_backtest_channel(), run_paper_channel()):
            for label, sequence in expected.items():
                assert channel.types_of(label) == sequence, (
                    f"{channel.name}/{label}: {[e.value for e in channel.types_of(label)]}"
                )
