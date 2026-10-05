"""``MiniQMTGateway``: the live ExecutionPort channel backed by miniQMT.

The gateway is pure protocol translation and session management — no
strategy, no market view (per the execution design: 网关只做协议翻译与会话
管理).  It implements the full
:class:`~pulsar_contracts.execution.ExecutionPort` contract against:

* a :class:`~pulsar_exec.live.protocol.BrokerSession` (the local
  protocol/stub projection of ``xtquant.xttrader``; tests inject in-memory
  fakes, :mod:`pulsar_exec.live.xt_bridge` builds the real one), and
* a :class:`~pulsar_exec.live.gate.LiveGate` (unlock environment variable,
  per-order and daily caps, rejection trail).

Behaviour mandated by the design and implemented here:

* **Idempotency** — the intent key is resolved *before* anything reaches
  the broker; retries/replays return the original ``OrderId`` and never
  produce a duplicate order.
* **Unconfirmed outcomes stay unknown** — a submission or cancellation
  whose outcome cannot be confirmed (session down) emits ``ERROR``, leaves
  the order state untouched and converges later via ``poll``/reconciliation;
  it is never assumed failed and re-sent (绝不假设失败重报).
* **Channel-private states are digested** — wire statuses in flight are
  absorbed silently; only fill/cancel outcomes surface as events, all
  validated through the shared state machine
  (:func:`pulsar_exec.state_machine.advance_order`).
* **Reconnect = reconcile-first** — on disconnect the gateway goes
  read-only; on reconnect it pulls broker truth, converges and only
  resumes accepting intents when the reconciliation is clean, otherwise it
  enters the read-only alert state until an operator acknowledges.
"""

from __future__ import annotations

import enum
import threading
from collections.abc import Callable, Mapping
from datetime import datetime

from pulsar_contracts import (
    SHANGHAI_TZ,
    CancelResult,
    ExecutionEvent,
    ExecutionPort,
    Fill,
    Order,
    OrderId,
    OrderIntent,
    OrderState,
    Position,
    Side,
)

from .. import events as event_factory
from ..config import FeeSchedule
from ..fees import compute_fees
from ..idempotency import IdempotencyManager, default_order_id_factory
from ..state_machine import ExecutionStateError, advance_order
from .archive import EventArchive
from .gate import LiveGate
from .protocol import (
    BrokerSession,
    BrokerUnavailableError,
    UnsupportedPriceModeError,
    WireOrder,
    WirePhase,
    WireTrade,
    side_code_of,
    wire_phase,
    wire_price_type,
)
from .reconcile import Reconciler, ReconciliationReport

__all__ = ["GatewayState", "MiniQMTGateway"]


class GatewayState(enum.StrEnum):
    """Readiness of the gateway towards new intents."""

    ACCEPTING = "accepting"
    HALTED = "halted"  # safe shutdown: no new intents, cancels in flight
    DISCONNECTED = "disconnected"  # session down; read-only
    READ_ONLY_ALERT = "read_only_alert"  # reconciliation differences unconfirmed


def _default_clock() -> datetime:
    return datetime.now(tz=SHANGHAI_TZ)


class MiniQMTGateway(ExecutionPort):  # type: ignore[misc]  # contracts lack py.typed
    """Live trading gateway for one broker account and one run.

    Construct with the broker ``session`` (protocol fake in tests, the
    xtquant bridge in production), the mandatory :class:`LiveGate`, a run
    identifier and an optional JSONL event archive; then ``start()`` to
    connect and register push callbacks.  All order progress arrives
    through ``on_event`` callbacks, exactly like the backtest venue.
    """

    def __init__(
        self,
        *,
        session: BrokerSession,
        gate: LiveGate,
        run_id: str,
        fee_schedule: FeeSchedule | None = None,
        clock: Callable[[], datetime] | None = None,
        archive: EventArchive | None = None,
        order_id_factory: Callable[[], OrderId] = default_order_id_factory,
        baseline_positions: Mapping[str, int] | None = None,
        reference_price_provider: Callable[[str], float | None] | None = None,
        reconciler: Reconciler | None = None,
    ) -> None:
        if not run_id:
            raise ValueError("run_id must be non-empty")

        self._session = session
        self._gate = gate
        self._run_id = run_id
        self._fees = fee_schedule if fee_schedule is not None else FeeSchedule()
        self._clock = clock or _default_clock
        self._archive = archive
        self._idempotency = IdempotencyManager(order_id_factory)
        self._reference_price_provider = reference_price_provider
        self._reconciler = reconciler if reconciler is not None else Reconciler()
        self._has_position_baseline = baseline_positions is not None

        self._lock = threading.RLock()
        self._state = GatewayState.ACCEPTING
        self._alert_reason: str | None = None
        self._callbacks: list[Callable[[ExecutionEvent], None]] = []

        self._orders: dict[OrderId, Order] = {}
        self._wire_of: dict[OrderId, int] = {}
        self._order_of_wire: dict[int, OrderId] = {}
        self._applied_trade_ids: set[str] = set()
        self._applied_fills: list[Fill] = []
        self._unconfirmed_submissions: set[OrderId] = set()
        self._orphan_orders: dict[int, WireOrder] = {}
        self._orphan_trades: dict[str, WireTrade] = {}
        self._stale_pushes = 0
        self._local_positions: dict[str, int] = dict(baseline_positions or {})

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    @property
    def run_id(self) -> str:
        """Identifier of the live run this gateway serves."""
        return self._run_id

    @property
    def state(self) -> GatewayState:
        """Current readiness state (accepting / halted / read-only)."""
        with self._lock:
            return self._state

    @property
    def alert_reason(self) -> str | None:
        """Why the gateway sits in its read-only alert, if it does."""
        with self._lock:
            return self._alert_reason

    def start(self) -> None:
        """Connect the session and register push callbacks."""
        self._session.register_callbacks(self)
        self._session.connect()

    def close(self) -> None:
        """Disconnect the session (idempotent; safe shutdown does more)."""
        try:
            self._session.disconnect()
        except BrokerUnavailableError:  # pragma: no cover - already gone
            pass

    def halt(self, reason: str) -> None:
        """Safe-shutdown entry: refuse new intents (cancels still allowed)."""
        with self._lock:
            self._state = GatewayState.HALTED
        self._gate.halt(f"gateway halted: {reason}")

    # ------------------------------------------------------------------
    # ExecutionPort
    # ------------------------------------------------------------------
    def submit(self, intent: OrderIntent) -> OrderId:
        """Submit an intent through the live gate; idempotent on its key.

        Order of checks: idempotency replay, gateway readiness (folded into
        the gate trail), the three gate rejections (locked / per-order cap
        / daily cap), price-mode translation, then the broker call.  An
        unavailable session leaves the order ``CREATED`` with an ``ERROR``
        event — unknown, not failed.
        """
        with self._lock:
            registration = self._idempotency.register(intent)
            if not registration.created:
                return registration.order_id

            order_id = registration.order_id
            now = self._clock()
            order = Order.from_intent(order_id, intent, created_at=now)
            self._orders[order_id] = order

            reference_price = self._gate_reference_price(intent)
            decision = self._gate.check(intent, reference_price=reference_price)
            if not decision.allowed:
                reason = decision.reason or "live gate rejected the intent"
                self._emit(order, event_factory.rejected(order_id, now, reason))
                return order_id

            try:
                price_type = wire_price_type(intent.price_mode.value)
            except UnsupportedPriceModeError as exc:
                self._emit(order, event_factory.rejected(order_id, now, str(exc)))
                return order_id

            try:
                wire_id = self._session.place_order(
                    symbol=intent.symbol,
                    side_code=side_code_of(intent.side.value),
                    quantity=intent.quantity,
                    price_type=price_type,
                    price=float(intent.limit_price or 0.0),
                )
            except BrokerUnavailableError as exc:
                self._unconfirmed_submissions.add(order_id)
                self._emit(
                    order,
                    event_factory.error(
                        order_id,
                        now,
                        f"order submission unconfirmed ({exc}); state kept and "
                        f"converging via reconciliation",
                    ),
                )
                return order_id

            if wire_id < 0:
                self._emit(
                    order,
                    event_factory.rejected(
                        order_id,
                        now,
                        "broker session refused the order (negative acknowledgement)",
                    ),
                )
                return order_id

            self._wire_of[order_id] = wire_id
            self._order_of_wire[wire_id] = order_id
            # The session may push the fresh (in-flight) record from inside
            # place_order, before this mapping existed; such a record was
            # buffered as an orphan and belongs to this order after all.
            self._orphan_orders.pop(wire_id, None)
            self._unconfirmed_submissions.discard(order_id)
            self._emit(order, event_factory.accepted(order_id, now))
            return order_id

    def cancel(self, order_id: OrderId) -> CancelResult:
        """Request cancellation; unconfirmed cancels converge via queries."""
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return CancelResult(
                    order_id=order_id, accepted=False, reason="unknown order_id"
                )
            if order.status.is_terminal:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    status=order.status,
                    reason="order already terminal",
                )
            wire_id = self._wire_of.get(order_id)
            if wire_id is None:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    status=order.status,
                    reason="order not yet acknowledged by broker; unconfirmed "
                    "submission converges via reconciliation",
                )
            try:
                acknowledgement = self._session.cancel_order(wire_id)
            except BrokerUnavailableError:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    status=order.status,
                    reason="cancel outcome unconfirmed (session unavailable); "
                    "order state unknown and converges via reconciliation",
                )
            if acknowledgement < 0:
                return CancelResult(
                    order_id=order_id,
                    accepted=False,
                    status=order.status,
                    reason="broker refused the cancellation (possibly already "
                    "filled)",
                )
            return CancelResult(order_id=order_id, accepted=True, status=order.status)

    def query(self, order_id: OrderId) -> OrderState:
        """Local snapshot of one order (reconciliation entry point)."""
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
        """Broker-side positions (the broker is the authority when live)."""
        wires = self._session.query_positions()
        return [
            Position(
                symbol=wire.symbol,
                quantity=wire.quantity,
                available_quantity=wire.available_quantity,
                avg_cost=wire.avg_cost,
            )
            for wire in wires
        ]

    def on_event(self, callback: Callable[[ExecutionEvent], None]) -> None:
        """Register a callback receiving every execution event."""
        self._callbacks.append(callback)

    # ------------------------------------------------------------------
    # Broker push callbacks (BrokerCallbacks implementation)
    # ------------------------------------------------------------------
    def on_order_push(self, record: WireOrder) -> None:
        """Digest one pushed order record (channel-private states absorbed)."""
        with self._lock:
            self._apply_order_record(record)

    def on_trade_push(self, record: WireTrade) -> None:
        """Digest one pushed trade record into a fill event (idempotent)."""
        with self._lock:
            self._apply_trade(record)

    def on_disconnected(self) -> None:
        """Session dropped: read-only immediately, nothing assumed."""
        with self._lock:
            self._state = GatewayState.DISCONNECTED
        self._gate.halt(
            "broker session disconnected; gateway read-only until reconciled"
        )

    def on_reconnected(self) -> None:
        """Session back: reconcile first, resume only when clean."""
        with self._lock:
            try:
                self._poll_locked()
            except BrokerUnavailableError:
                return  # still gone; a later reconnect retries
            report = self._reconciler.reconcile(self)
            if report.ok:
                self._state = GatewayState.ACCEPTING
                self._alert_reason = None
                self._gate.resume()
            else:
                self._state = GatewayState.READ_ONLY_ALERT
                self._alert_reason = report.summary()
                self._gate.halt(
                    f"read-only alert: {len(report.entries)} reconciliation "
                    f"difference(s) require operator confirmation"
                )

    # ------------------------------------------------------------------
    # Reconciliation surface
    # ------------------------------------------------------------------
    def poll(self) -> None:
        """Pull broker truth and converge local order/fill state onto it."""
        with self._lock:
            self._poll_locked()

    def reconcile(self) -> ReconciliationReport:
        """Converge on broker truth, then diff; stores an alert when dirty.

        Delegates to :class:`~pulsar_exec.live.reconcile.Reconciler`; the
        returned report is the post-market reconciliation report when the
        market is closed and the reconnect report after a drop.
        """
        with self._lock:
            self._poll_locked()
            report = self._reconciler.reconcile(self)
            if not report.ok:
                self._state = GatewayState.READ_ONLY_ALERT
                self._alert_reason = report.summary()
                self._gate.halt(
                    f"read-only alert: {len(report.entries)} reconciliation "
                    f"difference(s) require operator confirmation"
                )
            return report

    def acknowledge_alert(self, note: str) -> bool:
        """Operator confirmation of reconciliation differences (直至确认).

        Returns ``True`` when an alert was cleared; the note is mandatory —
        an unexplained acknowledgement is not an audit trail.
        """
        if not note:
            raise ValueError("acknowledgement note must be non-empty")
        with self._lock:
            if self._alert_reason is None:
                return False
            self._alert_reason = None
            self._state = GatewayState.ACCEPTING
        self._gate.resume()
        return True

    # ------------------------------------------------------------------
    # Introspection (shutdown manifest / audit)
    # ------------------------------------------------------------------
    def order_book(self) -> tuple[Order, ...]:
        """Snapshots of every order the gateway ever saw."""
        with self._lock:
            return tuple(self._orders.values())

    def active_orders(self) -> tuple[Order, ...]:
        """Orders that are not terminal yet (cancel candidates)."""
        with self._lock:
            return tuple(o for o in self._orders.values() if not o.status.is_terminal)

    def wire_id_of(self, order_id: OrderId) -> int | None:
        """The broker-side wire id of an order, if acknowledged."""
        with self._lock:
            return self._wire_of.get(order_id)

    @property
    def gate(self) -> LiveGate:
        """The live gate instance guarding this gateway."""
        return self._gate

    @property
    def session(self) -> BrokerSession:
        """The broker session this gateway talks to (reconciliation input)."""
        return self._session

    @property
    def stale_push_count(self) -> int:
        """Pushes that arrived out of order and were safely absorbed."""
        with self._lock:
            return self._stale_pushes

    @property
    def unconfirmed_submissions(self) -> tuple[OrderId, ...]:
        """Orders whose submission outcome is still unknown."""
        with self._lock:
            return tuple(self._unconfirmed_submissions)

    @property
    def applied_fills(self) -> tuple[Fill, ...]:
        """Every fill applied to the local book, oldest first."""
        with self._lock:
            return tuple(self._applied_fills)

    @property
    def orphan_orders(self) -> dict[int, WireOrder]:
        """Broker order records that map to no gateway order."""
        with self._lock:
            return dict(self._orphan_orders)

    @property
    def orphan_trade_ids(self) -> frozenset[str]:
        """Ids of broker trades that could not be attributed locally."""
        with self._lock:
            return frozenset(self._orphan_trades)

    @property
    def has_position_baseline(self) -> bool:
        """Whether a position baseline was provided at construction."""
        return self._has_position_baseline

    def reconciliation_clock(self) -> datetime:
        """Current gateway clock (timestamps reconciliation reports)."""
        return self._clock()

    def local_position_quantities(self) -> dict[str, int]:
        """Fill-derived position quantities (reconciliation input)."""
        with self._lock:
            return dict(self._local_positions)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _gate_reference_price(self, intent: OrderIntent) -> float | None:
        if intent.limit_price is not None:
            return float(intent.limit_price)
        if self._reference_price_provider is not None:
            return self._reference_price_provider(intent.symbol)
        return None

    def _poll_locked(self) -> None:
        # Trades first: terminal order statuses then find fills applied.
        for trade in self._session.query_trades():
            self._apply_trade(trade)
        for order_record in self._session.query_orders():
            self._apply_order_record(order_record)

    def _order_by_wire(self, wire_order_id: int) -> Order | None:
        order_id = self._order_of_wire.get(wire_order_id)
        return None if order_id is None else self._orders[order_id]

    def _apply_trade(self, record: WireTrade) -> None:
        if record.trade_id in self._applied_trade_ids:
            return
        order = self._order_by_wire(record.wire_order_id)
        if order is None:
            # Unknown wire id: a trade we cannot attribute (yet).  Never
            # fabricate a fill for it — reconciliation reports the orphan.
            self._orphan_trades.setdefault(record.trade_id, record)
            return
        if order.status.is_terminal:
            self._stale_pushes += 1
            return
        cumulative = order.filled_quantity + record.quantity
        if cumulative > order.quantity:
            # Broker reports more volume than the order carries: do not
            # apply, flag via reconciliation instead of guessing.
            self._stale_pushes += 1
            return

        fees = compute_fees(
            price=record.price,
            quantity=record.quantity,
            side=order.side,
            schedule=self._fees,
        )
        fill = Fill(
            fill_id=f"xt-{record.trade_id}",
            order_id=order.order_id,
            symbol=record.symbol,
            side=order.side,
            price=record.price,
            quantity=record.quantity,
            commission=fees.commission,
            stamp_duty=fees.stamp_duty,
            transfer_fee=fees.transfer_fee,
            ts=datetime.fromtimestamp(record.ts_epoch, tz=SHANGHAI_TZ),
        )
        fill_event = (
            event_factory.fill(order.order_id, fill.ts, fill)
            if cumulative == order.quantity
            else event_factory.partial_fill(order.order_id, fill.ts, fill)
        )
        self._emit(order, fill_event)
        self._applied_trade_ids.add(record.trade_id)
        self._applied_fills.append(fill)
        self._gate.record_fill(round(record.price * record.quantity, 2))
        self._move_local_position(order.side, order.symbol, record.quantity)

    def _apply_order_record(self, record: WireOrder) -> None:
        order = self._order_by_wire(record.wire_order_id)
        if order is None:
            self._orphan_orders.setdefault(record.wire_order_id, record)
            return
        if order.status.is_terminal:
            self._stale_pushes += 1
            return

        phase = wire_phase(record.status_code)
        now = self._clock()

        if phase in (WirePhase.IN_FLIGHT, WirePhase.PARTIALLY_FILLED):
            # Channel-private progress: trades surface as fill events on
            # their own; a wire-filled quantity without trade records is
            # deferred to reconciliation (never fabricated here).
            return
        if phase is WirePhase.FILLED:
            # Completion must come from trades; the terminal FILL event is
            # emitted by _apply_trade.  Nothing to do on the order push.
            return
        if phase is WirePhase.CANCELLED:
            if record.filled_quantity > order.filled_quantity:
                # Part-filled cancel without the fill records yet: defer;
                # reconciliation pulls trades and finishes the lifecycle.
                return
            self._emit(
                order,
                event_factory.cancelled(
                    order.order_id,
                    now,
                    reason=(
                        f"broker cancelled the order "
                        f"({record.status_msg or record.status_code})"
                    ),
                ),
            )
            return
        # WirePhase.UNKNOWN: unconfirmed — state unchanged, converge later.
        self._emit(
            order,
            event_factory.error(
                order.order_id,
                now,
                f"unknown broker status code {record.status_code} "
                f"({record.status_msg!r}); state kept, converging via "
                f"reconciliation",
            ),
        )

    def _move_local_position(self, side: Side, symbol: str, quantity: int) -> None:
        delta = quantity if side is Side.BUY else -quantity
        self._local_positions[symbol] = self._local_positions.get(symbol, 0) + delta

    def _emit(self, order: Order, event: ExecutionEvent) -> None:
        """Advance the snapshot through the shared state machine, archive,
        then dispatch to every registered callback."""
        try:
            advanced = advance_order(order, event)
        except ExecutionStateError:
            # A stale/duplicate push cannot corrupt the lifecycle; it is
            # absorbed here and surfaces in reconciliation counts.
            self._stale_pushes += 1
            return
        self._orders[advanced.order_id] = advanced
        if self._archive is not None:
            self._archive.append(event)
        for callback in self._callbacks:
            callback(event)
