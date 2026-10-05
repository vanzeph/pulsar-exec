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
  :class:`~pulsar_contracts.common.OrderId`;
* :mod:`pulsar_exec.config` — run configuration for the training channel:
  fee schedule, slippage model and bar-matching rules (all rates
  configuration with policy defaults, never hard-coded policy);
* :mod:`pulsar_exec.fees` — per-fill commission / stamp duty / transfer
  fee computation in exact decimal arithmetic;
* :mod:`pulsar_exec.slippage` — fixed-bps + optional volume-impact
  slippage penalty on matched prices;
* :mod:`pulsar_exec.price_limit` — A-share daily price-limit bands
  (main ±10%, GEM/STAR ±20%, ST ±5%) and one-line-board (一字板)
  detection;
* :mod:`pulsar_exec.account` — simulated cash account with T+1 sellable
  positions;
* :mod:`pulsar_exec.venue` — :class:`BacktestVenue`, the event-driven
  bar-level matching engine implementing
  :class:`~pulsar_contracts.execution.ExecutionPort` for training runs.

Paper broker and live broker gateways arrive in later deliveries.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .account import BacktestAccount, SymbolLot
from .config import (
    DEFAULT_BOARD_LIMIT_RATES,
    FeeSchedule,
    MatchingRules,
    SlippageModel,
)
from .events import accepted, cancelled, error, fill, partial_fill, rejected
from .fees import FeeBreakdown, compute_fees
from .idempotency import (
    IdempotencyConflictError,
    IdempotencyManager,
    SubmitRegistration,
    default_order_id_factory,
)
from .price_limit import is_one_line_board, limit_prices, limit_rate_for
from .slippage import apply_slippage
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
from .venue import DEFAULT_INSTRUMENT, BacktestVenue

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
    # configuration
    "DEFAULT_BOARD_LIMIT_RATES",
    "FeeSchedule",
    "SlippageModel",
    "MatchingRules",
    # fees / slippage / price limits
    "FeeBreakdown",
    "compute_fees",
    "apply_slippage",
    "limit_prices",
    "limit_rate_for",
    "is_one_line_board",
    # account
    "BacktestAccount",
    "SymbolLot",
    # venue
    "BacktestVenue",
    "DEFAULT_INSTRUMENT",
]
