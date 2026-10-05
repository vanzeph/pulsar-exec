"""``PaperBroker``: the realtime paper-trading channel (实时模拟盘).

The Pulsar execution design defines the paper channel as *realtime
snapshot driven with a local ledger* (实时快照驱动 + 本地账本): it exists
for intraday integration and signal validation, touches no real money and
shares its semantics with the training channel component by component:

* **Account** — the very same :class:`~pulsar_exec.account.BacktestAccount`
  the backtest venue books against (cash, positions, T+1 available
  quantities, per-fill fee accrual), so ledger behaviour cannot diverge
  between training and paper runs;
* **State machine** — every event is applied through
  :func:`~pulsar_exec.state_machine.advance_order`, the single shared
  order lifecycle validator;
* **Pre-trade validation** — the shared
  :func:`~pulsar_exec.validation.validate_intent` (board lots, funding,
  T+1 availability, odd-lot one-shot sells, price-limit band);
* **Price limits** — the same
  :func:`~pulsar_exec.price_limit.limit_prices` bands and
  :func:`~pulsar_exec.price_limit.is_one_line_board` sealed-board (一字板)
  rule, the latter evaluated against a bar synthesized from the snapshot.

Driving model (the upper layer injects the stream — this class never
subscribes by itself and depends only on ``pulsar-contracts``):

* :meth:`PaperBroker.start` / :meth:`PaperBroker.stop` — session
  lifecycle; intents are only accepted while the session runs, stopping
  cancels every active order;
* :meth:`PaperBroker.on_snapshot` — feed one realtime
  :class:`~pulsar_contracts.market_data.Snapshot` (five-level book,
  monotonic per-symbol ``seq``); resting orders on that symbol are
  matched immediately. Late or duplicated snapshots (``seq`` not
  increasing) are dropped silently — delivery gaps are flagged by the
  data side and never raise here; gaps in the sequence are tolerated;
* :meth:`PaperBroker.on_session_end` — close a trading day: expire
  remaining ``DAY`` orders, fix the session close as the next day's
  previous close (for bands and sealed-board detection) and roll the T+1
  availability bucket;
* :meth:`PaperBroker.set_previous_close` / :meth:`PaperBroker.mark_suspended`
  — seed yesterday's close before the open and flag suspensions.

Matching (严格模式, per the design "撮合采用严格模式（按买卖档位判定）"):

1. **Strict boundary** — a limit buy fills only against ask levels
   *strictly cheaper* than the limit (the book must penetrate the limit,
   mirroring the backtest's ``strict_price_boundary`` semantics); a limit
   sell only against bid levels *strictly dearer*. Construction refuses
   non-strict rules: paper is strict by design. Fills happen at the
   displayed level prices, so a penetrating book yields price improvement.
2. **Sealed boards & suspensions** — a snapshot whose last price sits on
   the limit band (one-line board, detected via the reused E2 rule
   against a synthesized bar) or a suspended symbol never fills, in
   either direction.
3. **Marketable intents** — counter-price orders take the best
   counter level per snapshot (remainder rests); five-level-IOC sweeps
   the displayed levels and cancels whatever remains on the same
   snapshot (即成剩撤), including when nothing was fillable at all.
4. **Displayed liquidity** — fills consume the displayed level volumes;
   several orders competing on one snapshot share those volumes in
   submission (FIFO) order, mirroring the venue's per-bar participation
   bookkeeping.
5. **Fees** — commission/stamp duty/transfer fee are booked per fill via
   :func:`~pulsar_exec.fees.compute_fees`, exactly like the venue. No
   synthetic slippage is applied: the displayed book is the reality a
   paper run validates against.

Event trail (事件留痕): every emitted event passes through the optional
:class:`~pulsar_exec.live.archive.EventArchive` (JSONL, append-only)
before the ``on_event`` callbacks see it, and the session lifecycle
itself is recorded in an inspectable session trail.
"""

from __future__ import annotations

import enum
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Final

from pulsar_contracts import (
    SHANGHAI_TZ,
    Bar,
    Board,
    CancelResult,
    Exchange,
    ExecutionEvent,
    ExecutionPort,
    Fill,
    Freq,
    Instrument,
    Order,
    OrderId,
    OrderIntent,
    OrderState,
    Position,
    PriceMode,
    Side,
    Snapshot,
    TimeInForce,
)

from . import events as event_factory
from .account import BacktestAccount
from .config import FeeSchedule, MatchingRules
from .fees import compute_fees
from .idempotency import IdempotencyManager
from .live.archive import EventArchive
from .price_limit import is_one_line_board
from .state_machine import advance_order
from .validation import affordable_quantity, validate_intent

__all__ = ["PaperBroker", "PaperSessionState", "PaperSessionRecord", "DEFAULT_INSTRUMENT"]

#: Fallback instrument for symbols the session did not register: a healthy
#: main-board stock, so price-limit bands apply the ±10% / ST ±5% rules.
DEFAULT_INSTRUMENT = Instrument(
    symbol="<default>",
    exchange=Exchange.SSE,
    board=Board.MAIN,
    is_st=False,
    list_date=date(1990, 12, 19),
)

_SESSION_CLOSE_TZ = time(15, 0)

#: Maximum order-book depth per side carried by a snapshot (五档).
_MAX_LEVELS: Final[int] = 5


def _session_close_ts(day: date) -> datetime:
    return datetime.combine(day, _SESSION_CLOSE_TZ, tzinfo=SHANGHAI_TZ)


def _synthesize_bar(snapshot: Snapshot) -> Bar:
    """Project a snapshot onto a single-price bar for the sealed-board rule.

    ``is_one_line_board`` is a bar-level rule of the shared E2 engine; a
    snapshot whose market sits on one price becomes the degenerate bar
    ``O == H == L == C == last_price`` and is judged by exactly that rule.
    """
    return Bar(
        symbol=snapshot.symbol,
        ts=snapshot.ts,
        freq=Freq.MINUTE,
        open=snapshot.last_price,
        high=snapshot.last_price,
        low=snapshot.last_price,
        close=snapshot.last_price,
        volume=snapshot.volume,
        amount=snapshot.amount,
    )


class PaperSessionState(enum.StrEnum):
    """Lifecycle states of one paper session."""

    IDLE = "idle"  # constructed, not started yet
    RUNNING = "running"  # accepting intents, matching snapshots
    STOPPED = "stopped"  # terminal: stopped, active orders cancelled


@dataclass(frozen=True)
class PaperSessionRecord:
    """One lifecycle mark of the session trail (事件留痕).

    ``kind`` is ``"started"``, ``"session_end"`` or ``"stopped"``;
    ``detail`` carries the day (session end) or the stop reason.
    """

    kind: str
    ts: datetime
    detail: str = ""


class PaperBroker(ExecutionPort):  # type: ignore[misc]  # contracts lack py.typed
    """Realtime snapshot-driven paper broker with a local ledger.

    Construct one broker per paper run with its configuration and
    starting cash; register instruments (boards drive price limits);
    ``start()`` the session, then drive it with ``on_snapshot`` while the
    upper layer pumps the realtime stream, and ``stop()`` when done. All
    order progress arrives through ``on_event`` callbacks, exactly like
    the backtest venue and the live gateway.
    """

    def __init__(
        self,
        *,
        initial_cash: float = 1_000_000.0,
        fee_schedule: FeeSchedule | None = None,
        matching: MatchingRules | None = None,
        instruments: Iterable[Instrument] = (),
        clock: datetime | None = None,
        archive: EventArchive | None = None,
    ) -> None:
        if initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        rules = matching if matching is not None else MatchingRules()
        if not rules.strict_price_boundary:
            raise ValueError(
                "PaperBroker matches in strict mode (design mandate: 撮合采用"
                "严格模式); pass MatchingRules(strict_price_boundary=True)"
            )
        self._account = BacktestAccount(cash=round(initial_cash, 2))
        self._fees = fee_schedule or FeeSchedule()
        self._matching = rules
        self._instruments: dict[str, Instrument] = {
            instrument.symbol: instrument for instrument in instruments
        }
        self._idempotency = IdempotencyManager()
        self._archive = archive

        self._orders: dict[OrderId, Order] = {}
        self._active: list[OrderId] = []  # submission order, FIFO matching
        self._callbacks: list[Callable[[ExecutionEvent], None]] = []
        self._lock = threading.RLock()

        self._state = PaperSessionState.IDLE
        self._clock: datetime | None = clock
        self._trail: list[PaperSessionRecord] = []

        self._prev_close: dict[str, float] = {}
        self._last_price: dict[str, float] = {}
        self._session_close: dict[str, float] = {}
        self._last_seq: dict[str, int] = {}
        self._dropped_stale = 0
        self._suspended: set[tuple[str, date]] = set()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------
    @property
    def state(self) -> PaperSessionState:
        """Current lifecycle state of the paper session."""
        with self._lock:
            return self._state

    def start(self, now: datetime | None = None) -> None:
        """Open the session: intents are accepted, snapshots drive matching.

        Idempotent while running; a stopped session is terminal — build a
        new broker for the next run. ``now`` optionally fixes the broker
        clock (otherwise the constructor clock or the first snapshot
        provides it).
        """
        with self._lock:
            if self._state is PaperSessionState.RUNNING:
                return
            if self._state is PaperSessionState.STOPPED:
                raise RuntimeError(
                    "paper session already stopped; construct a new "
                    "PaperBroker for the next session"
                )
            if now is not None:
                self._clock = now
            self._state = PaperSessionState.RUNNING
            self._record("started")

    def stop(self, reason: str = "session stopped") -> None:
        """Terminate the session, cancelling every active order.

        Idempotent. Each cancelled order emits a ``CANCELLED`` event
        (audited like every other event) before the session stops
        accepting anything.
        """
        with self._lock:
            if self._state is not PaperSessionState.RUNNING:
                return
            now = self._now()
            for order_id in list(self._active):
                self._deactivate(order_id)
                self._emit(
                    self._orders[order_id],
                    event_factory.cancelled(
                        order_id, now, reason=f"paper session stopped: {reason}"
                    ),
                )
            self._state = PaperSessionState.STOPPED
            self._record("stopped", detail=reason)

    def session_trail(self) -> list[PaperSessionRecord]:
        """Lifecycle marks recorded so far (start / session ends / stop)."""
        with self._lock:
            return list(self._trail)

    # ------------------------------------------------------------------
    # ExecutionPort
    # ------------------------------------------------------------------
    def submit(self, intent: OrderIntent) -> OrderId:
        """Submit an intent; idempotent on its key.

        Retries/replays of a known key return the original ``OrderId``
        without re-validating or re-emitting events. New intents are
        accepted only while the session runs and pass the same pre-trade
        validation as the backtest venue; the outcome is an ``ACCEPTED``
        or ``REJECTED`` event.
        """
        with self._lock:
            registration = self._idempotency.register(intent)
            if not registration.created:
                return registration.order_id

            now = self._now()
            order = Order.from_intent(registration.order_id, intent, created_at=now)
            self._orders[order.order_id] = order

            if self._state is not PaperSessionState.RUNNING:
                self._emit(
                    order,
                    event_factory.rejected(
                        order.order_id,
                        now,
                        f"paper session is {self._state.value}; start() before "
                        "submitting intents",
                    ),
                )
                return order.order_id

            reason = validate_intent(
                intent,
                account=self._account,
                instrument=self._instrument(intent.symbol),
                matching=self._matching,
                fees=self._fees,
                reference_price=self._reference_price(intent),
                prev_close=self._prev_close.get(intent.symbol),
            )
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
    # Snapshot driving (injected by the upper layer)
    # ------------------------------------------------------------------
    def on_snapshot(self, snapshot: Snapshot) -> None:
        """Match resting orders of ``snapshot.symbol`` against one book.

        Snapshots are best effort: a ``seq`` that does not advance over
        the last seen one (late or duplicated delivery) is dropped
        without raising, and gaps in the sequence are tolerated — the
        data side flags gaps, the paper ledger just keeps converging on
        the freshest book it saw.
        """
        with self._lock:
            if self._state is not PaperSessionState.RUNNING:
                return  # outside a live session no market data is consumed

            last_seq = self._last_seq.get(snapshot.symbol)
            if last_seq is not None and snapshot.seq <= last_seq:
                self._dropped_stale += 1
                return

            self._last_seq[snapshot.symbol] = snapshot.seq
            self._clock = snapshot.ts
            self._last_price[snapshot.symbol] = snapshot.last_price
            self._session_close[snapshot.symbol] = snapshot.last_price

            if (snapshot.symbol, snapshot.ts.date()) in self._suspended:
                return  # 停牌不可成交

            instrument = self._instrument(snapshot.symbol)
            prev_close = self._prev_close.get(snapshot.symbol)
            if prev_close is not None and is_one_line_board(
                _synthesize_bar(snapshot),
                prev_close,
                board=instrument.board,
                is_st=instrument.is_st,
                rules=self._matching,
            ):
                return  # 一字板不可成交

            # mutable working copies: orders filling on this snapshot
            # consume the displayed liquidity in FIFO order
            asks = [[level.price, level.volume] for level in snapshot.asks]
            bids = [[level.price, level.volume] for level in snapshot.bids]

            for order_id in list(self._active):
                if self._orders[order_id].symbol != snapshot.symbol:
                    continue
                self._match_order(order_id, snapshot, asks, bids)

    def on_session_end(self, day: date) -> None:
        """Close ``day``: expire DAY orders, fix prev closes, roll T+1."""
        with self._lock:
            if self._state is not PaperSessionState.RUNNING:
                return  # a stopped session consumes no market timeline
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
            self._record("session_end", detail=str(day))

    def set_previous_close(self, symbol: str, prev_close: float) -> None:
        """Seed yesterday's close (drives bands and sealed-board detection).

        The driver calls this before the open when it knows the reference
        price; without it the broker learns the previous close from the
        first session end it observes.
        """
        if prev_close <= 0:
            raise ValueError("prev_close must be positive")
        with self._lock:
            self._prev_close[symbol] = prev_close

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
        """Current cash balance of the paper account."""
        return self._account.cash

    @property
    def clock(self) -> datetime | None:
        """Timestamp of the last observed snapshot/session event."""
        return self._clock

    @property
    def dropped_stale_snapshots(self) -> int:
        """How many late/duplicated snapshots were dropped so far."""
        return self._dropped_stale

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _instrument(self, symbol: str) -> Instrument:
        return self._instruments.get(symbol, DEFAULT_INSTRUMENT)

    def _now(self) -> datetime:
        if self._clock is None:
            raise RuntimeError(
                "paper clock is unset; construct with clock=, start(now=...) "
                "or drive on_snapshot first"
            )
        return self._clock

    def _record(self, kind: str, detail: str = "") -> None:
        if self._clock is not None:
            self._trail.append(
                PaperSessionRecord(kind=kind, ts=self._clock, detail=detail)
            )

    def _reference_price(self, intent: OrderIntent) -> float | None:
        """Price used for the funds estimate of a marketable buy."""
        if intent.limit_price is not None:
            return float(intent.limit_price)
        return self._last_price.get(intent.symbol)

    def _match_order(
        self,
        order_id: OrderId,
        snapshot: Snapshot,
        asks: list[list[float]],
        bids: list[list[float]],
    ) -> None:
        """Match one order against the working copies of the book.

        Limit orders sweep the counter levels whose price penetrates the
        limit (strict semantics); counter-price orders take the best
        level only; five-level-IOC sweeps every displayed level and
        cancels its remainder on the spot. Buy tranches are clipped to
        the affordable quantity, sell tranches to the T+1 available one.
        The tranches of one snapshot coalesce into a single blended fill
        (volume-weighted level prices) — one fill event per order and
        snapshot, mirroring the venue's one-fill-per-bar granularity.
        """
        strict = self._matching.strict_price_boundary
        levels = asks if self._orders[order_id].side is Side.BUY else bids
        tranches: list[tuple[float, int]] = []
        committed = 0.0  # value already swept by earlier tranches

        for index, level in enumerate(levels[:_MAX_LEVELS]):
            order = self._orders[order_id]
            remaining = order.quantity - order.filled_quantity
            if remaining <= 0:
                break
            price, volume = level[0], level[1]
            if volume <= 0:
                continue

            if order.price_mode is PriceMode.LIMIT:
                limit = order.limit_price
                assert limit is not None  # guarded by the contract layer
                if order.side is Side.BUY:
                    crossed = price < limit if strict else price <= limit
                else:
                    crossed = price > limit if strict else price >= limit
                if not crossed:
                    break  # levels are ordered: deeper cannot cross either
            elif order.price_mode is PriceMode.COUNTER_PRICE and index > 0:
                break  # counter price takes the best level only

            take = min(remaining, int(volume))
            if order.side is Side.BUY:
                take = affordable_quantity(
                    self._account.cash - committed, price, take, self._fees
                )
            else:
                take = min(take, self._account.available_quantity(order.symbol))
            if take <= 0:
                break  # no cash (buy) or no sellable shares left (sell)

            tranches.append((price, take))
            committed += price * take
            level[1] -= take

        if tranches:
            quantity = sum(take for _price, take in tranches)
            value = sum(Decimal(str(price)) * take for price, take in tranches)
            blended = float(value / Decimal(quantity))
            self._execute(self._orders[order_id], blended, quantity, snapshot.ts)

        if self._orders[order_id].status.is_terminal:
            self._deactivate(order_id)
        elif self._orders[order_id].price_mode is PriceMode.FIVE_LEVEL_CANCEL_REMAINDER:
            # 即成剩撤: whatever the five levels could not absorb is
            # cancelled on the spot, in the same snapshot.
            self._deactivate(order_id)
            self._emit(
                self._orders[order_id],
                event_factory.cancelled(
                    order_id,
                    snapshot.ts,
                    reason="five-level IOC remainder cancelled",
                ),
            )

    def _execute(self, order: Order, fill_price: float, quantity: int, ts: datetime) -> None:
        """Execute ``quantity`` shares of ``order`` at ``fill_price``.

        Books fees per fill, updates the T+1 account and emits the fill
        event through the shared state machine (``FILL`` when the order
        completes, ``PARTIAL_FILL`` otherwise). The displayed level price
        is used as-is — a paper run applies no synthetic slippage.
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
            ts=ts,
        )
        if order.side is Side.BUY:
            self._account.apply_buy(order.symbol, quantity, fill_price, fees)
        else:
            self._account.apply_sell(order.symbol, quantity, fill_price, fees)

        completing = order.filled_quantity + quantity == order.quantity
        fill_event = (
            event_factory.fill(order.order_id, ts, fill)
            if completing
            else event_factory.partial_fill(order.order_id, ts, fill)
        )
        self._emit(order, fill_event)

    def _deactivate(self, order_id: OrderId) -> None:
        try:
            self._active.remove(order_id)
        except ValueError:  # pragma: no cover - defensive double deactivation
            pass

    def _emit(self, order: Order, event: ExecutionEvent) -> None:
        """Advance the order snapshot through the shared state machine,
        archive the event (留痕) and dispatch it to every callback."""
        advanced = advance_order(order, event)
        self._orders[advanced.order_id] = advanced
        if self._archive is not None:
            self._archive.append(event)
        for callback in self._callbacks:
            callback(event)
