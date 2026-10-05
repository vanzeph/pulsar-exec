"""pulsar-exec: the execution layer of the Pulsar A-share trading system.

This package implements the execution-side semantics that every channel
(backtest venue, paper broker, live gateway) shares, on top of the
``pulsar-contracts`` port definitions:

* :mod:`pulsar_exec.state_machine` — the order state machine validator
  (legal-transition whitelist, illegal-transition rejection, event-driven
  advancement of immutable order snapshots);
* :mod:`pulsar_exec.events` — canonical constructors for
  :class:`~pulsar_contracts.execution.ExecutionEvent` objects;
* :mod:`pulsar_exec.idempotency` — idempotency-key management so that
  re-submitting the same ``run id + seq`` key returns the same
  :class:`~pulsar_contracts.common.OrderId`.

Concrete venues/brokers/gateways live in later deliveries; this module
intentionally contains no channel logic.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .events import accepted, cancelled, error, fill, partial_fill, rejected
from .idempotency import (
    IdempotencyConflictError,
    IdempotencyManager,
    SubmitRegistration,
    default_order_id_factory,
)
from .state_machine import (
    EVENT_ALLOWED_CURRENT,
    EVENT_TARGET_STATUS,
    LEGAL_TRANSITIONS,
    ORDER_STATE_MACHINE,
    EventStateMismatchError,
    ExecutionStateError,
    FillConsistencyError,
    IllegalOrderTransitionError,
    OrderStateMachine,
    advance_order,
    validate_event_for_order,
)

try:
    __version__ = version("pulsar-exec")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0.dev0"

__all__ = [
    "__version__",
    # state machine
    "LEGAL_TRANSITIONS",
    "EVENT_TARGET_STATUS",
    "EVENT_ALLOWED_CURRENT",
    "OrderStateMachine",
    "ORDER_STATE_MACHINE",
    "advance_order",
    "validate_event_for_order",
    "ExecutionStateError",
    "IllegalOrderTransitionError",
    "EventStateMismatchError",
    "FillConsistencyError",
    # events
    "accepted",
    "rejected",
    "partial_fill",
    "fill",
    "cancelled",
    "error",
    # idempotency
    "IdempotencyConflictError",
    "IdempotencyManager",
    "SubmitRegistration",
    "default_order_id_factory",
]
