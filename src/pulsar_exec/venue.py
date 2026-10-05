"""``BacktestVenue``: the event-driven bar-level matching engine (training channel).

The venue implements the full :class:`~pulsar_contracts.execution.ExecutionPort`
contract — ``submit``/``cancel``/``query``/``positions``/``on_event`` — so the
core engine runs the same code against backtest, paper and live channels.
Order progress is reported exclusively as
:class:`~pulsar_contracts.execution.ExecutionEvent`s driven through the
shared state machine (:func:`pulsar_exec.state_machine.advance_order`),
which is what keeps the lifecycle semantics identical across channels.

Driving model (the replay loop calls these in order):

* :meth:`BacktestVenue.on_bar` — feed one historical bar; resting orders
  on that symbol are matched immediately (fillability below);
* :meth:`BacktestVenue.on_session_end` — close a trading day: expire
  remaining ``DAY`` orders, fix the session close as next day's previous
  close (for price-limit bands) and roll the T+1 availability bucket;
* :meth:`BacktestVenue.mark_suspended` — flag a symbol as suspended for a
  day; suspended symbols never fill (停牌不可成交).

Fillability (bar level, per the Pulsar execution design):

1. **Locked boards** — a one-line limit board (一字板, see
   :func:`pulsar_exec.price_limit.is_one_line_board`) and suspended
   symbols never fill, in either direction.
2. **Price boundary** — limit buys fill when the bar's low touches
   (default mode) or strictly penetrates (strict mode) the limit;
   symmetric for sells against the bar's high. The base fill price is
   ``min(open, limit)`` for buys / ``max(open, limit)`` for sells — an
   open away from the limit fills at the better opening price.
   Marketable intents (counter-price / five-level-IOC) fill at the bar
   close; a five-level-IOC cancels its remainder on the same bar
   (即成剩撤), including when nothing was fillable at all.
3. **Volume participation** — the total quantity orders may take from one
   bar is capped at ``max_volume_fraction`` of the bar's volume (default
   10%), leaving the rest of the bar to the market.
4. **Slippage & fees** — the base price is penalized by the configured
   :mod:`~pulsar_exec.slippage` model; commission/stamp duty/transfer fee
   are booked per fill via :mod:`~pulsar_exec.fees`.

Venue-side pre-trade validation (per the design, board-lot rounding and
funding checks live here, never in the strategy): buy orders must be
whole board lots; odd lots sell only as a one-shot sale of the entire
available position; sells cannot exceed T+1 available quantity; buys
cannot exceed cash (estimated at the limit price); limit prices must sit
inside the day's price-limit band once the previous close is known.
Violations are rejected with ``REJECTED`` events carrying the reason.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterable
from datetime import date, datetime, time

from pulsar_contracts import (
    SHANGHAI_TZ,
    Bar,
    Board,
    CancelResult,
    Exchange,
    ExecutionEvent,
    ExecutionPort,
    Fill,
    Instrument,
    InstrumentStatus,
    Order,
    OrderId,
    OrderIntent,
    OrderState,
    Position,
    PriceMode,
    Side,
    TimeInForce,
)

from . import events as event_factory
from .account import BacktestAccount
from .config import FeeSchedule, MatchingRules, SlippageModel
from .fees import compute_fees
from .idempotency import IdempotencyManager
from .price_limit import is_one_line_board, limit_prices
from .slippage import apply_slippage
from .state_machine import advance_order

__all__ = ["BacktestVenue", "DEFAULT_INSTRUMENT"]

#: Fallback instrument for symbols the run did not register: a healthy
#: main-board stock. Price-limit bands then apply the ±10% / ST ±5% rules.
DEFAULT_INSTRUMENT = Instrument(
    symbol="<default>",
    exchange=Exchange.SSE,
    board=Board.MAIN,
    is_st=False,
    list_date=date(1990, 12, 19),
)

_SESSION_CLOSE_TZ = time(15, 0)


def _session_close_ts(day: date) -> datetime:
    return datetime.combine(day, _SESSION_CLOSE_TZ, tzinfo=SHANGHAI_TZ)


class BacktestVenue(ExecutionPort):  # type: ignore[misc]  # contracts lack py.typed
    """Event-driven bar-level backtest matching engine.

    Construct one venue per run with its configuration and starting cash;
    register instruments (boards drive price limits); drive it with
    ``on_bar`` / ``on_session_end`` while the replay loop feeds historical
    bars. All order progress arrives through ``on_event`` callbacks.
    """

    def __init__(
        self,
        *,
        initial_cash: float = 1_000_000.0,
        fee_schedule: FeeSchedule | None = None,
        slippage: SlippageModel | None = None,
        matching: MatchingRules | None = None,
        instruments: Iterable[Instrument] = (),
        clock: datetime | None = None,
    ) -> None:
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        self._account = BacktestAccount(cash=round(initial_cash, 2))
        self._fees = fee_schedule or FeeSchedule()
        self._slippage = slippage if slippage is not None else SlippageModel()
        self._matching = matching if matching is not None else MatchingRules()
        self._instruments: dict[str, Instrument] = {
            instrument.symbol: instrument for instrument in instruments
        }
        self._idempotency = IdempotencyManager()

        self._orders: dict[OrderId, Order] = {}
        self._active: list[OrderId] = []  # submission order, FIFO matching
        self._callbacks: list[Callable[[ExecutionEvent], None]] = []
        self._lock = threading.RLock()

        self._clock: datetime | None = clock
        self._prev_close: dict[str, float] = {}
        self._session_close: dict[str, float] = {}
        self._bar_consumption: dict[tuple[str, datetime], int] = {}
        self._suspended: set[tuple[str, date]] = set()

    # ------------------------------------------------------------------
    # ExecutionPort
    # ------------------------------------------------------------------
    def submit(self, intent: OrderIntent) -> OrderId:
        """Submit an intent; idempotent on its key.

        Retries/replays of a known key return the original ``OrderId``
        without re-validating or re-emitting events. New intents are
        validated venue-side (board lots, T+1 availability, funds,
        price-limit band) and answered with ``ACCEPTED`` or ``REJECTED``.
        """
        with self._lock:
            registration = self._idempotency.register(intent)
            if not registration.created:
                return registration.order_id

            now = self._now()
            order = Order.from_intent(registration.order_id, intent, created_at=now)
            self._orders[order.order_id] = order

            reason = self._validate_intent(intent)
            if reason is not None:
                self._emit(order, event_factory.rejected(order.order_id, now, reason))
                return order.order_id

            self._active.append(order.order_id)
            self._emit(order, event_factory.accepted(order.order_id, now))
            return order.order_id

    def cancel(self, order_id: OrderId) -> CancelResult:
        """Request cancellation of an active order."""
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    reason="unknown order_id",
                )
            if order.status.is_terminal:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    status=order.status,
                    reason="order already terminal",
                )
            self._deactivate(order_id)
            self._emit(
                order,
                event_factory.cancelled(
                    order_id, self._now(), reason="cancel requested"
                ),
            )
            return CancelResult(
                order_id=order_id,
                accepted=True,
                status=self._orders[order_id].status,
            )

    def query(self, order_id: OrderId) -> OrderState:
        """Snapshot of one order's state (reconciliation entry point)."""
        order = self._orders.get(order_id)
        if order is None:
            raise ValueError(f"unknown order_id {order_id!r}")
        return OrderState(
            order_id=order.order_id,
            status=order.status,
            filled_quantity=order.filled_quantity,
            avg_fill_price=order.avg_fill_price,
            updated_at=order.updated_at,
        )

    def positions(self) -> list[Position]:
        """Current positions with T+1 available quantities."""
        with self._lock:
            return self._account.position_views()

    def on_event(self, callback: Callable[[ExecutionEvent], None]) -> None:
        """Register a callback receiving every execution event."""
        self._callbacks.append(callback)

    # ------------------------------------------------------------------
    # Replay-loop driving
    # ------------------------------------------------------------------
    def on_bar(self, bar: Bar) -> None:
        """Match resting orders of ``bar.symbol`` against one historical bar."""
        with self._lock:
            self._clock = bar.ts
            self._session_close[bar.symbol] = bar.close

            if (bar.symbol, bar.ts.date()) in self._suspended:
                return  # 停牌不可成交

            instrument = self._instrument(bar.symbol)
            prev_close = self._prev_close.get(bar.symbol)
            if is_one_line_board(
                bar,
                prev_close,
                board=instrument.board,
                is_st=instrument.is_st,
                rules=self._matching,
            ):
                return  # 一字板不可成交

            fillable = int(bar.volume * self._matching.max_volume_fraction)
            consumed = self._bar_consumption.get((bar.symbol, bar.ts), 0)

            for order_id in list(self._active):
                order = self._orders[order_id]
                if order.symbol != bar.symbol:
                    continue
                remaining = order.quantity - order.filled_quantity

                base_price = self._base_fill_price(order, bar)
                if base_price is not None:
                    take = min(remaining, fillable - consumed)
                    share = (take / bar.volume) if bar.volume > 0 else 0.0
                    if order.side is Side.BUY:
                        # affordability must hold at the slipped price (the
                        # price actually paid), not the raw matched price
                        provisional = apply_slippage(
                            reference_price=base_price,
                            side=Side.BUY,
                            model=self._slippage,
                            volume_share=share,
                        )
                        take = self._affordable(provisional, take)
                    else:
                        # a second sell accepted against the same available
                        # shares clips to whatever is still sellable
                        take = min(take, self._account.available_quantity(order.symbol))
                    if take > 0:
                        fill_price = apply_slippage(
                            reference_price=base_price,
                            side=order.side,
                            model=self._slippage,
                            volume_share=(take / bar.volume) if bar.volume > 0 else 0.0,
                        )
                        self._execute(order, fill_price, take, bar)
                        consumed += take

                completed = self._orders[order_id].status.is_terminal
                if completed:
                    self._deactivate(order_id)
                elif order.price_mode is PriceMode.FIVE_LEVEL_CANCEL_REMAINDER:
                    # 即成剩撤: whatever the five levels could not absorb is
                    # cancelled on the spot, in the same bar.
                    self._deactivate(order_id)
                    self._emit(
                        self._orders[order_id],
                        event_factory.cancelled(
                            order_id,
                            bar.ts,
                            reason="five-level IOC remainder cancelled",
                        ),
                    )

            if consumed:
                self._bar_consumption[(bar.symbol, bar.ts)] = consumed

    def on_session_end(self, day: date) -> None:
        """Close ``day``: expire DAY orders, fix prev closes, roll T+1."""
        with self._lock:
            now = _session_close_ts(day)
            self._clock = now
            for order_id in list(self._active):
                order = self._orders[order_id]
                if order.time_in_force is TimeInForce.DAY:
                    self._deactivate(order_id)
                    self._emit(
                        order,
                        event_factory.cancelled(
                            order_id,
                            now,
                            reason="day order expired at session close",
                        ),
                    )
            for symbol, close in self._session_close.items():
                self._prev_close[symbol] = close
            self._session_close.clear()
            self._account.roll_trading_day()

    def mark_suspended(self, symbol: str, day: date) -> None:
        """Flag ``symbol`` as suspended on ``day`` (fills are impossible)."""
        self._suspended.add((symbol, day))

    def is_suspended(self, symbol: str, day: date) -> bool:
        """Whether ``symbol`` was marked suspended on ``day``."""
        return (symbol, day) in self._suspended

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    @property
    def cash(self) -> float:
        """Current cash balance of the simulated account."""
        return self._account.cash

    @property
    def clock(self) -> datetime | None:
        """Timestamp of the last observed bar/session event."""
        return self._clock

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _instrument(self, symbol: str) -> Instrument:
        return self._instruments.get(symbol, DEFAULT_INSTRUMENT)

    def _now(self) -> datetime:
        if self._clock is None:
            raise RuntimeError(
                "venue clock is unset; construct with clock= or drive on_bar first"
            )
        return self._clock

    def _validate_intent(self, intent: OrderIntent) -> str | None:
        """Venue-side pre-trade validation; ``None`` means accepted."""
        instrument = self._instrument(intent.symbol)
        if instrument.status is InstrumentStatus.SUSPENDED:
            return f"{intent.symbol} is suspended (停牌)"

        if intent.side is Side.BUY:
            lot = self._matching.lot_size
            if intent.quantity % lot != 0:
                return (
                    f"buy quantity {intent.quantity} is not a multiple of the "
                    f"{lot}-share board lot"
                )
            reference = self._reference_price(intent)
            if reference is None:
                return (
                    "cannot estimate funds: no limit price and no market price "
                    "seen for this symbol yet"
                )
            estimate = compute_fees(
                price=reference,
                quantity=intent.quantity,
                side=Side.BUY,
                schedule=self._fees,
            )
            cost = round(reference * intent.quantity + estimate.total, 2)
            if cost > self._account.cash + 0.005:
                return (
                    f"insufficient funds: estimated cost {cost:.2f} exceeds "
                    f"cash {self._account.cash:.2f}"
                )
        else:
            available = self._account.available_quantity(intent.symbol)
            if intent.quantity > available:
                return (
                    f"sell quantity {intent.quantity} exceeds T+1 available "
                    f"{available} of {intent.symbol}"
                )
            lot = self._matching.lot_size
            if available % lot != 0 and intent.quantity != available:
                return (
                    f"odd-lot position of {available} shares must be sold in "
                    "one shot (零股一次性卖出)"
                )

        if intent.price_mode is PriceMode.LIMIT:
            prev_close = self._prev_close.get(intent.symbol)
            if prev_close is not None:
                limit_up, limit_down = limit_prices(
                    prev_close,
                    board=instrument.board,
                    is_st=instrument.is_st,
                    rules=self._matching,
                )
                if not (limit_down - 0.005 <= intent.limit_price <= limit_up + 0.005):
                    return (
                        f"limit price {intent.limit_price:.2f} outside the day's "
                        f"price-limit band [{limit_down:.2f}, {limit_up:.2f}]"
                    )
        return None

    def _reference_price(self, intent: OrderIntent) -> float | None:
        """Price used for the funds estimate of a marketable buy."""
        if intent.limit_price is not None:
            return float(intent.limit_price)
        return self._prev_close.get(intent.symbol) or self._session_close.get(
            intent.symbol
        )

    def _base_fill_price(self, order: Order, bar: Bar) -> float | None:
        """Base (pre-slippage) fill price, or ``None`` when not fillable.

        Limit orders are judged against the bar boundary (touch-or-cross
        in default mode, strict penetration in strict mode) and fill at
        the better of open and limit when the open is already through the
        limit. Marketable intents (counter price, five-level IOC) fill at
        the bar close — A-shares have no classic market order.
        """
        strict = self._matching.strict_price_boundary
        if order.price_mode is PriceMode.LIMIT:
            limit = order.limit_price
            if order.side is Side.BUY:
                touched = bar.low < limit if strict else bar.low <= limit
                if not touched:
                    return None
                return float(min(bar.open, limit))
            touched = bar.high > limit if strict else bar.high >= limit
            if not touched:
                return None
            return float(max(bar.open, limit))
        return float(bar.close)

    def _affordable(self, price: float, desired: int) -> int:
        """Largest quantity ≤ ``desired`` payable with current cash.

        Accounts for the commission minimum via the actual fee function;
        the analytic starting point leaves at most a couple of correction
        steps, so the decrement loop is bounded and tiny.
        """
        schedule = self._fees
        combined = schedule.commission_rate + schedule.transfer_fee_rate

        def cost(qty: int) -> float:
            fees = compute_fees(
                price=price, quantity=qty, side=Side.BUY, schedule=schedule
            )
            return round(price * qty + fees.total, 2)

        candidate = int(self._account.cash / (price * (1 + combined)))
        if schedule.min_commission > 0:
            by_minimum = int(
                (self._account.cash - schedule.min_commission)
                / (price * (1 + schedule.transfer_fee_rate))
            )
            candidate = max(candidate, by_minimum)
        qty = max(0, min(desired, candidate))
        while qty > 0 and cost(qty) > self._account.cash + 0.005:
            qty -= 1
        return qty

    def _execute(self, order: Order, fill_price: float, quantity: int, bar: Bar) -> None:
        """Execute ``quantity`` shares of ``order`` at ``fill_price``.

        Books fees per fill, updates the T+1 account and emits the fill
        event through the shared state machine (``FILL`` when the order
        completes, ``PARTIAL_FILL`` otherwise). ``fill_price`` already
        carries the slippage penalty.
        """
        fees = compute_fees(
            price=fill_price,
            quantity=quantity,
            side=order.side,
            schedule=self._fees,
        )
        fill = Fill(
            fill_id=f"fill-{order.order_id}-{order.filled_quantity + quantity}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            price=fill_price,
            quantity=quantity,
            commission=fees.commission,
            stamp_duty=fees.stamp_duty,
            transfer_fee=fees.transfer_fee,
            ts=bar.ts,
        )
        if order.side is Side.BUY:
            self._account.apply_buy(order.symbol, quantity, fill_price, fees)
        else:
            self._account.apply_sell(order.symbol, quantity, fill_price, fees)

        completing = order.filled_quantity + quantity == order.quantity
        fill_event = (
            event_factory.fill(order.order_id, bar.ts, fill)
            if completing
            else event_factory.partial_fill(order.order_id, bar.ts, fill)
        )
        self._emit(order, fill_event)

    def _deactivate(self, order_id: OrderId) -> None:
        try:
            self._active.remove(order_id)
        except ValueError:  # pragma: no cover - defensive double deactivation
            pass

    def _emit(self, order: Order, event: ExecutionEvent) -> None:
        """Advance the order snapshot through the shared state machine and
        dispatch the event to every registered callback."""
        advanced = advance_order(order, event)
        self._orders[advanced.order_id] = advanced
        for callback in self._callbacks:
            callback(event)
