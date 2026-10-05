"""Live-trading channel of pulsar-exec: miniQMT gateway, gate, safety.

Deliverables of the live milestone (Pulsar execution design):

* :mod:`pulsar_exec.live.protocol` — the local ``BrokerSession``/
  ``BrokerCallbacks`` protocol (stub) isolating the gateway from the
  xtquant SDK, plus the wire records and status-code mapping;
* :mod:`pulsar_exec.live.gate` — :class:`LiveGate`: unlock environment
  variable, per-order and daily caps, append-only rejection trail;
* :mod:`pulsar_exec.live.gateway` — :class:`MiniQMTGateway`, the live
  ``ExecutionPort`` implementation (submit/cancel/query/positions/events,
  push digestion, unconfirmed-outcome semantics, reconcile-first
  reconnect);
* :mod:`pulsar_exec.live.reconcile` — post-market and reconnect
  reconciliation with a persisted difference report;
* :mod:`pulsar_exec.live.shutdown` — :func:`safe_shutdown` (cancel active
  orders, refuse new intents, write the shutdown manifest);
* :mod:`pulsar_exec.live.archive` — JSONL event archive (订单事件与回报落盘);
* :mod:`pulsar_exec.live.xt_bridge` — the only xtquant reference, dynamic
  and terminal-bound (not importable without miniQMT installed).
"""

from __future__ import annotations

from .archive import EventArchive
from .gate import (
    GateDecision,
    GateRejectCode,
    LiveGate,
    LiveGateConfig,
    RejectionRecord,
)
from .gateway import GatewayState, MiniQMTGateway
from .protocol import (
    BrokerUnavailableError,
    BrokerCallbacks,
    BrokerSession,
    MiniQMTUnavailableError,
    UnsupportedPriceModeError,
    WireAsset,
    WireOrder,
    WirePhase,
    WirePosition,
    WireTrade,
    wire_phase,
)
from .reconcile import (
    DifferenceEntry,
    DifferenceKind,
    ReconciliationReport,
    Reconciler,
    Severity,
    run_post_market_reconciliation,
)
from .shutdown import OrderSnapshot, ShutdownManifest, safe_shutdown

__all__ = [
    # protocol / stub
    "BrokerSession",
    "BrokerCallbacks",
    "BrokerUnavailableError",
    "UnsupportedPriceModeError",
    "MiniQMTUnavailableError",
    "WireOrder",
    "WireTrade",
    "WirePosition",
    "WireAsset",
    "WirePhase",
    "wire_phase",
    # gate
    "LiveGate",
    "LiveGateConfig",
    "GateDecision",
    "GateRejectCode",
    "RejectionRecord",
    # gateway
    "MiniQMTGateway",
    "GatewayState",
    # reconciliation
    "Reconciler",
    "ReconciliationReport",
    "DifferenceEntry",
    "DifferenceKind",
    "Severity",
    "run_post_market_reconciliation",
    # shutdown
    "safe_shutdown",
    "ShutdownManifest",
    "OrderSnapshot",
    # archive
    "EventArchive",
]
