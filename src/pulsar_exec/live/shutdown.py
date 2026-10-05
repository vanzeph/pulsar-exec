"""Safe shutdown: cancel live orders, refuse new intents, persist state.

The Pulsar execution design mandates the failure behaviour implemented
here (安全停机): when the gateway or the core hits an exception, the run
must first cancel every active order, then stop accepting new intents,
and finally write its state into a manifest artefact — the exec-side
contribution to the run's RunManifest archive.

Unconfirmed cancellations are *recorded, not assumed*: an order whose
cancel outcome could not be confirmed stays in the manifest's
``unresolved`` list with its outcome marked ``unconfirmed``; the
lifecycle converges later via reconciliation (绝不假设失败重报).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

from pulsar_contracts import Order, OrderId

from .gateway import GatewayState, MiniQMTGateway
from .protocol import BrokerUnavailableError
from .reconcile import ReconciliationReport

__all__ = [
    "OrderOutcome",
    "OrderSnapshot",
    "ShutdownManifest",
    "safe_shutdown",
]


def _outcome_of(result: object) -> str:
    """Classify a :class:`~pulsar_contracts.execution.CancelResult`."""
    accepted = bool(getattr(result, "accepted", False))
    reason = str(getattr(result, "reason", "") or "")
    if accepted:
        return "confirmed"
    if "unconfirmed" in reason:
        return "unconfirmed"
    if "not yet acknowledged" in reason:
        return "not_submitted"
    return "refused"


@dataclass(frozen=True)
class OrderSnapshot:
    """Manifest view of one order at shutdown time."""

    order_id: str
    symbol: str
    side: str
    quantity: int
    filled_quantity: int
    status: str
    wire_order_id: int | None
    cancel_outcome: str

    @classmethod
    def from_gateway(
        cls,
        order_id: OrderId,
        gateway: MiniQMTGateway,
        cancel_outcome: str,
    ) -> "OrderSnapshot":
        order: Order = next(
            o for o in gateway.order_book() if o.order_id == order_id
        )
        return cls(
            order_id=str(order.order_id),
            symbol=order.symbol,
            side=order.side.value,
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            status=order.status.value,
            wire_order_id=gateway.wire_id_of(order.order_id),
            cancel_outcome=cancel_outcome,
        )


@dataclass(frozen=True)
class ShutdownManifest:
    """Persisted state of a safely shut-down live run."""

    run_id: str
    ts: datetime
    reason: str
    gateway_state: str
    day_traded_value: float
    rejection_count: int
    stale_push_count: int
    alert_reason: str | None
    reconciliation_deferred: bool
    reconciliation_entries: int
    orders: tuple[OrderSnapshot, ...] = field(default_factory=tuple)
    unresolved: tuple[OrderSnapshot, ...] = field(default_factory=tuple)

    @property
    def clean(self) -> bool:
        """True when no order was left non-terminal."""
        return not self.unresolved

    def to_payload(self) -> dict[str, object]:
        """JSON-ready payload of the manifest."""
        return {
            "run_id": self.run_id,
            "ts": self.ts.isoformat(),
            "reason": self.reason,
            "gateway_state": self.gateway_state,
            "day_traded_value": round(self.day_traded_value, 2),
            "rejection_count": self.rejection_count,
            "stale_push_count": self.stale_push_count,
            "alert_reason": self.alert_reason,
            "reconciliation_deferred": self.reconciliation_deferred,
            "reconciliation_entries": self.reconciliation_entries,
            "clean": self.clean,
            "orders": [asdict(order) for order in self.orders],
            "unresolved": [o.order_id for o in self.unresolved],
        }

    def write(self, directory: Path | str) -> Path:
        """Persist the manifest under ``directory``; returns its path."""
        stamp = self.ts.strftime("%Y%m%dT%H%M%S")
        target = Path(directory) / f"shutdown-{self.run_id}-{stamp}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_payload(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return target


def safe_shutdown(
    gateway: MiniQMTGateway,
    *,
    reason: str,
    manifest_dir: Path | str | None = None,
) -> ShutdownManifest:
    """Run the safe-shutdown sequence and return its manifest.

    Steps (in order): halt new intents, cancel every active order, poll
    and reconcile to converge statuses (deferring when the session is
    down), disconnect the session, persist the manifest.  The gateway
    ends in :attr:`GatewayState.HALTED` (or the stricter read-only alert
    when reconciliation found differences).
    """
    if not reason:
        raise ValueError("shutdown reason must be non-empty")

    # 1. Stop new intents: every further submit is refused with a trail.
    gateway.halt(reason)

    # 2. Cancel all active orders; classify each outcome honestly.
    outcomes: dict[OrderId, str] = {}
    for order in gateway.active_orders():
        outcomes[order.order_id] = _outcome_of(gateway.cancel(order.order_id))

    # 3. Converge on broker truth so cancels/fills are reflected locally.
    deferred = False
    entries = 0
    alert_reason: str | None = None
    report: ReconciliationReport | None = None
    try:
        gateway.poll()
    except BrokerUnavailableError:
        deferred = True
    try:
        report = gateway.reconcile()
        entries = len(report.entries)
    except BrokerUnavailableError:
        deferred = True

    state = gateway.state
    if state is GatewayState.ACCEPTING or state is GatewayState.DISCONNECTED:
        state = GatewayState.HALTED
    if report is not None and not report.ok:
        alert_reason = gateway.alert_reason or report.summary()

    # 4. Close the session (best effort).
    gateway.close()

    snapshots = tuple(
        OrderSnapshot.from_gateway(order_id, gateway, outcomes.get(order_id, "n/a"))
        for order_id in (order.order_id for order in gateway.order_book())
    )
    unresolved = tuple(
        OrderSnapshot.from_gateway(order.order_id, gateway, outcomes.get(order.order_id, "n/a"))
        for order in gateway.active_orders()
    )

    manifest = ShutdownManifest(
        run_id=gateway.run_id,
        ts=gateway.reconciliation_clock(),
        reason=reason,
        gateway_state=state.value,
        day_traded_value=gateway.gate.day_traded_value,
        rejection_count=len(gateway.gate.rejections),
        stale_push_count=gateway.stale_push_count,
        alert_reason=alert_reason,
        reconciliation_deferred=deferred,
        reconciliation_entries=entries,
        orders=snapshots,
        unresolved=unresolved,
    )
    if manifest_dir is not None:
        manifest.write(manifest_dir)
    return manifest
