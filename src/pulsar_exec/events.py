"""Construction of :class:`~pulsar_contracts.execution.ExecutionEvent` objects.

The contract layer already enforces the payload shape (fill required on
``PARTIAL_FILL``/``FILL`` and bound to the event's order, reason required on
``REJECTED``/``ERROR``, no fill elsewhere). This module gives channels one
canonical constructor per event kind so every venue emits structurally
identical events:

* :func:`accepted` — the gateway accepted the order (``CREATED -> SUBMITTED``);
* :func:`rejected` — pre-trade validation failed (``CREATED -> REJECTED``);
* :func:`partial_fill` — an execution leaving a remainder;
* :func:`fill` — the final execution completing the order;
* :func:`cancelled` — cancellation succeeded, remainder voided;
* :func:`error` — an unconfirmed outcome; the order keeps its current
  status and converges via ``query``/reconciliation.

Semantic (state-machine-aware) validation of a constructed event against a
live order lives in :mod:`pulsar_exec.state_machine`
(:func:`~pulsar_exec.state_machine.validate_event_for_order`).
"""

from __future__ import annotations

from datetime import datetime

from pulsar_contracts import (
    ExecutionEvent,
    ExecutionEventType,
    Fill,
    OrderId,
)

__all__ = [
    "accepted",
    "rejected",
    "partial_fill",
    "fill",
    "cancelled",
    "error",
]


def accepted(order_id: OrderId, ts: datetime) -> ExecutionEvent:
    """Build an ``ACCEPTED`` event: gateway accepted the order."""
    return ExecutionEvent(
        event_type=ExecutionEventType.ACCEPTED,
        order_id=order_id,
        ts=ts,
    )


def rejected(order_id: OrderId, ts: datetime, reason: str) -> ExecutionEvent:
    """Build a ``REJECTED`` event: intent validation failed."""
    if not reason:
        raise ValueError("rejected() requires a non-empty reason")
    return ExecutionEvent(
        event_type=ExecutionEventType.REJECTED,
        order_id=order_id,
        ts=ts,
        reason=reason,
    )


def partial_fill(order_id: OrderId, ts: datetime, fill: Fill) -> ExecutionEvent:
    """Build a ``PARTIAL_FILL`` event carrying an execution with remainder."""
    return ExecutionEvent(
        event_type=ExecutionEventType.PARTIAL_FILL,
        order_id=order_id,
        ts=ts,
        fill=fill,
    )


def fill(order_id: OrderId, ts: datetime, fill: Fill) -> ExecutionEvent:
    """Build a ``FILL`` event: the execution completing the order."""
    return ExecutionEvent(
        event_type=ExecutionEventType.FILL,
        order_id=order_id,
        ts=ts,
        fill=fill,
    )


def cancelled(order_id: OrderId, ts: datetime, reason: str | None = None) -> ExecutionEvent:
    """Build a ``CANCELLED`` event; the optional reason says why."""
    return ExecutionEvent(
        event_type=ExecutionEventType.CANCELLED,
        order_id=order_id,
        ts=ts,
        reason=reason,
    )


def error(order_id: OrderId, ts: datetime, reason: str) -> ExecutionEvent:
    """Build an ``ERROR`` event: unconfirmed outcome, state unchanged."""
    if not reason:
        raise ValueError("error() requires a non-empty reason")
    return ExecutionEvent(
        event_type=ExecutionEventType.ERROR,
        order_id=order_id,
        ts=ts,
        reason=reason,
    )
