"""Reconciliation: converge on broker truth, then diff and report.

Two mandated uses (Pulsar execution design):

* **Post-market (盘后对账)** — after the close, the local book (orders,
  fills, positions) is compared against the broker's order/trade/position
  queries; every difference becomes a report entry and blocks the next
  Live start until an operator acknowledges
  (:meth:`MiniQMTGateway.acknowledge_alert`).
* **Reconnect** — after a session drop, the same machinery runs *before*
  the gateway resumes accepting intents; convergence-able differences
  (fills that happened while disconnected) are applied first, and only a
  clean comparison reopens the gate (对账不一致进入只读告警态).

Convergence-before-diff is the key ordering: unknown local state is first
advanced onto broker truth (via the gateway's poll/digest path), so the
remaining entries are *genuine* differences — including orders the broker
never saw, which are reported as unknown and never assumed failed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

from pulsar_contracts import OrderStatus

from .protocol import (
    WireOrder,
    WirePhase,
    WirePosition,
    WireTrade,
    wire_phase,
)

if TYPE_CHECKING:  # pragma: no cover - typing only, no runtime import
    from .gateway import MiniQMTGateway

__all__ = [
    "DifferenceKind",
    "Severity",
    "DifferenceEntry",
    "ReconciliationReport",
    "Reconciler",
    "run_post_market_reconciliation",
]


class DifferenceKind(Enum):
    """What kind of local-vs-broker difference was found."""

    ORDER_UNKNOWN_AT_BROKER = "order_unknown_at_broker"
    ORDER_STATUS_MISMATCH = "order_status_mismatch"
    FILL_QUANTITY_MISMATCH = "fill_quantity_mismatch"
    MISSING_TRADE_RECORDS = "missing_trade_records"
    ORPHAN_BROKER_ORDER = "orphan_broker_order"
    ORPHAN_TRADE = "orphan_trade"
    POSITION_MISMATCH = "position_mismatch"
    CASH_MISMATCH = "cash_mismatch"


class Severity(Enum):
    """How hard a difference blocks the next Live session."""

    INFO = "info"
    WARNING = "warning"
    HIGH = "high"


@dataclass(frozen=True)
class DifferenceEntry:
    """One difference between the local book and broker truth."""

    kind: DifferenceKind
    severity: Severity
    detail: str
    order_id: str | None = None
    symbol: str | None = None


@dataclass(frozen=True)
class ReconciliationReport:
    """The outcome of one reconciliation run (盘后对账报告).

    ``ok`` means the local book and broker truth agree completely; any
    entry keeps the gateway in its read-only alert until acknowledged.
    """

    run_id: str
    ts: datetime
    entries: tuple[DifferenceEntry, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        """No differences at all."""
        return not self.entries

    @property
    def blocking(self) -> bool:
        """Whether any entry is more than informational."""
        return any(entry.severity is not Severity.INFO for entry in self.entries)

    def summary(self) -> str:
        """One-line human summary."""
        if self.ok:
            return f"reconciliation of run {self.run_id!r}: consistent"
        kinds = ", ".join(
            f"{entry.kind.value}({entry.severity.value})" for entry in self.entries
        )
        return (
            f"reconciliation of run {self.run_id!r}: "
            f"{len(self.entries)} difference(s): {kinds}"
        )

    def to_payload(self) -> dict[str, object]:
        """JSON-ready payload of the report."""
        return {
            "run_id": self.run_id,
            "ts": self.ts.isoformat(),
            "ok": self.ok,
            "blocking": self.blocking,
            "summary": self.summary(),
            "entries": [
                {
                    "kind": entry.kind.value,
                    "severity": entry.severity.value,
                    "detail": entry.detail,
                    "order_id": entry.order_id,
                    "symbol": entry.symbol,
                }
                for entry in self.entries
            ],
        }

    def write(self, path: Path | str) -> Path:
        """Persist the report as JSON (the post-market artefact)."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_payload(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return target


def _terminal_wire(record: WireOrder) -> bool:
    return wire_phase(record.status_code) in (WirePhase.FILLED, WirePhase.CANCELLED)


class Reconciler:
    """Stateless comparator of gateway book vs broker queries.

    Construct one with the optional cash expectation; ``reconcile`` reads
    the gateway's (already converged) book and the broker queries, and
    returns the difference report.  The gateway polls first — this class
    never mutates state.
    """

    def __init__(
        self,
        *,
        expected_initial_cash: float | None = None,
        cash_tolerance: float = 0.05,
    ) -> None:
        self._expected_initial_cash = expected_initial_cash
        self._cash_tolerance = cash_tolerance

    def reconcile(self, gateway: MiniQMTGateway) -> ReconciliationReport:
        """Diff the gateway book against broker truth."""
        entries: list[DifferenceEntry] = []
        broker_orders = {
            record.wire_order_id: record for record in gateway.session.query_orders()
        }
        broker_trades = gateway.session.query_trades()
        broker_positions = {
            position.symbol: position for position in gateway.session.query_positions()
        }

        entries.extend(self._order_entries(gateway, broker_orders))
        entries.extend(self._trade_entries(gateway, broker_trades))
        entries.extend(self._position_entries(gateway, broker_positions))
        entries.extend(self._cash_entries(gateway))

        return ReconciliationReport(
            run_id=gateway.run_id,
            ts=gateway.reconciliation_clock(),
            entries=tuple(entries),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _order_entries(
        self,
        gateway: MiniQMTGateway,
        broker_orders: dict[int, WireOrder],
    ) -> list[DifferenceEntry]:
        entries: list[DifferenceEntry] = []
        unconfirmed = set(gateway.unconfirmed_submissions)
        for order in gateway.order_book():
            wire_id = gateway.wire_id_of(order.order_id)
            if wire_id is None:
                # No wire id: the submission never completed.  A gate
                # rejection is terminal and fine; an order still CREATED
                # (or explicitly unconfirmed) has an unknown outcome that
                # must be reported, never assumed failed.
                if (
                    order.order_id in unconfirmed
                    or order.status is OrderStatus.CREATED
                ):
                    entries.append(
                        DifferenceEntry(
                            kind=DifferenceKind.ORDER_UNKNOWN_AT_BROKER,
                            severity=Severity.HIGH,
                            detail=(
                                "submission outcome unknown: the broker book "
                                "does not contain the order; kept as unknown, "
                                "never assumed failed nor re-sent"
                            ),
                            order_id=str(order.order_id),
                        )
                    )
                continue
            record = broker_orders.get(wire_id)
            if record is None:
                if not order.status.is_terminal:
                    entries.append(
                        DifferenceEntry(
                            kind=DifferenceKind.ORDER_UNKNOWN_AT_BROKER,
                            severity=Severity.HIGH,
                            detail=(
                                "acknowledged order missing from the broker "
                                "book while non-terminal locally"
                            ),
                            order_id=str(order.order_id),
                        )
                    )
                continue

            if order.filled_quantity != record.filled_quantity:
                entries.append(
                    DifferenceEntry(
                        kind=DifferenceKind.FILL_QUANTITY_MISMATCH,
                        severity=(
                            Severity.HIGH
                            if record.filled_quantity > order.filled_quantity
                            else Severity.WARNING
                        ),
                        detail=(
                            f"filled quantity differs: local "
                            f"{order.filled_quantity} vs broker "
                            f"{record.filled_quantity}"
                        ),
                        order_id=str(order.order_id),
                        symbol=order.symbol,
                    )
                )
            elif (
                not order.status.is_terminal
                and _terminal_wire(record)
                and order.status is not OrderStatus.REJECTED
            ):
                entries.append(
                    DifferenceEntry(
                        kind=DifferenceKind.MISSING_TRADE_RECORDS,
                        severity=Severity.HIGH,
                        detail=(
                            "broker reports a terminal state the gateway cannot "
                            "reconstruct (fill records missing); lifecycle left "
                            "open pending trade details"
                        ),
                        order_id=str(order.order_id),
                        symbol=order.symbol,
                    )
                )
            elif order.status.is_terminal and not _terminal_wire(record):
                entries.append(
                    DifferenceEntry(
                        kind=DifferenceKind.ORDER_STATUS_MISMATCH,
                        severity=Severity.HIGH,
                        detail=(
                            f"local status {order.status.value} is terminal but "
                            f"broker reports live status {record.status_code}"
                        ),
                        order_id=str(order.order_id),
                        symbol=order.symbol,
                    )
                )
        return entries

    def _trade_entries(
        self, gateway: MiniQMTGateway, broker_trades: list[WireTrade]
    ) -> list[DifferenceEntry]:
        entries: list[DifferenceEntry] = []
        orphans = gateway.orphan_trade_ids
        for trade in broker_trades:
            if trade.trade_id in orphans:
                entries.append(
                    DifferenceEntry(
                        kind=DifferenceKind.ORPHAN_TRADE,
                        severity=Severity.HIGH,
                        detail=(
                            f"broker trade {trade.trade_id} on wire order "
                            f"{trade.wire_order_id} cannot be attributed to any "
                            f"gateway order (manual activity on the account?)"
                        ),
                        symbol=trade.symbol,
                    )
                )
        for record in gateway.orphan_orders.values():
            entries.append(
                DifferenceEntry(
                    kind=DifferenceKind.ORPHAN_BROKER_ORDER,
                    severity=Severity.WARNING,
                    detail=(
                        f"broker order {record.wire_order_id} "
                        f"({record.symbol} qty {record.quantity}) was not "
                        f"originated by this run (manual activity on the "
                        f"account?)"
                    ),
                    symbol=record.symbol,
                )
            )
        return entries

    def _position_entries(
        self,
        gateway: MiniQMTGateway,
        broker_positions: dict[str, WirePosition],
    ) -> list[DifferenceEntry]:
        entries: list[DifferenceEntry] = []
        local = gateway.local_position_quantities()
        comparable = gateway.has_position_baseline
        # Without a baseline, positions this run never touched are not
        # comparable (the account may hold anything else); with one, every
        # broker-side symbol is checked.
        symbols = set(local) | (set(broker_positions) if comparable else set())
        for symbol in sorted(symbols):
            record = broker_positions.get(symbol)
            broker_qty = record.quantity if record is not None else 0
            if local.get(symbol, 0) != broker_qty:
                entries.append(
                    DifferenceEntry(
                        kind=DifferenceKind.POSITION_MISMATCH,
                        severity=Severity.HIGH,
                        detail=(
                            f"position differs: local {local.get(symbol, 0)} vs "
                            f"broker {broker_qty} shares"
                        ),
                        symbol=symbol,
                    )
                )
        return entries

    def _cash_entries(self, gateway: MiniQMTGateway) -> list[DifferenceEntry]:
        if self._expected_initial_cash is None:
            return []
        expected = self._expected_initial_cash
        for fill in gateway.applied_fills:
            value = fill.price * fill.quantity
            fees = fill.commission + fill.stamp_duty + fill.transfer_fee
            expected += (
                (value - fees) if fill.side.value == "sell" else -(value + fees)
            )
        broker_cash = gateway.session.query_asset().cash
        if abs(expected - broker_cash) > self._cash_tolerance:
            return [
                DifferenceEntry(
                    kind=DifferenceKind.CASH_MISMATCH,
                    severity=Severity.HIGH,
                    detail=(
                        f"cash differs: expected {expected:.2f} vs broker "
                        f"{broker_cash:.2f} CNY"
                    ),
                )
            ]
        return []


def run_post_market_reconciliation(
    gateway: MiniQMTGateway,
    path: Path | str,
) -> ReconciliationReport:
    """盘后对账: reconcile and persist the report artefact."""
    report = gateway.reconcile()
    report.write(path)
    return report
