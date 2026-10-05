"""Local protocol/stub layer isolating the gateway from the xtquant SDK.

The miniQMT terminal (and therefore its ``xtquant`` package) only exists on
machines with the broker's client installed; this repository must import,
typecheck and test cleanly everywhere else.  The live gateway is therefore
built against the *structural* :class:`BrokerSession` protocol defined
here — a faithful, minimal projection of the ``xtquant.xttrader`` surface
Pulsar needs:

======================  =============================================
Protocol method         xtquant origin
======================  =============================================
``connect``             ``XtTrader.connect``
``disconnect``          ``XtTrader.stop``
``register_callbacks``  ``XtTrader.subscribe``
``place_order``         ``XtTrader.order_stock``
``cancel_order``        ``XtTrader.cancel_order_stock_async``
``query_orders``        ``XtTrader.query_stock_orders``
``query_trades``       ``XtTrader.query_stock_trades``
``query_positions``    ``XtTrader.query_stock_positions``
``query_asset``        ``XtTrader.query_stock_asset``
======================  =============================================

The single runtime bridge that turns a real ``XtTrader`` into a
:class:`BrokerSession` lives in :mod:`pulsar_exec.live.xt_bridge` and is the
only module allowed to reference ``xtquant`` (dynamically, never at import
time).  Tests use in-memory fakes implementing this protocol, so the whole
gateway — including reconnect/reconciliation semantics — is verified
offline.

Wire value objects (``wire`` prefix) mirror the ``xtquant.xttype`` records
with clean names; the numeric status/side/price-type codes below re-declare
the documented ``xtquant.xtconstant`` values so the adapter needs no SDK
import to translate them onto the Pulsar order state machine.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

__all__ = [
    "BrokerUnavailableError",
    "UnsupportedPriceModeError",
    "MiniQMTUnavailableError",
    "WireOrder",
    "WireTrade",
    "WirePosition",
    "WireAsset",
    "WirePhase",
    "wire_phase",
    "side_code_of",
    "wire_price_type",
    "WIRE_ORDER_UNREPORTED",
    "WIRE_ORDER_WAIT_REPORT",
    "WIRE_ORDER_REPORTED",
    "WIRE_ORDER_PART_FILLED",
    "WIRE_ORDER_ALL_FILLED",
    "WIRE_ORDER_PART_CANCELLED",
    "WIRE_ORDER_ALL_CANCELLED",
    "WIRE_ORDER_SENDED",
    "WIRE_SIDE_BUY",
    "WIRE_SIDE_SELL",
    "WIRE_PRICE_FIX",
    "BrokerSession",
    "BrokerCallbacks",
]


class BrokerUnavailableError(RuntimeError):
    """The broker session is down and an operation outcome is UNKNOWN.

    Raised (never silently swallowed) by :class:`BrokerSession`
    implementations when a request could not be confirmed.  Per the Pulsar
    execution design an unconfirmed order/cancel must be treated as
    *unknown* and converged via query/reconciliation — never assumed failed
    and re-submitted.
    """


class UnsupportedPriceModeError(ValueError):
    """The intent's price mode has no miniQMT translation in this delivery."""

    def __init__(self, price_mode: str) -> None:
        self.price_mode = price_mode
        super().__init__(
            f"price mode {price_mode!r} has no miniQMT wire translation in the "
            f"first live delivery; only limit orders (PriceMode.LIMIT) are "
            f"supported against miniQMT"
        )


class MiniQMTUnavailableError(RuntimeError):
    """The xtquant SDK / miniQMT terminal is not usable on this machine."""


# ---------------------------------------------------------------------------
# Wire constants (mirroring xtquant.xtconstant; re-declared locally so the
# adapter never imports the SDK).  Values follow the documented miniQMT
# order-status codes, e.g. 48=未报, 49=待报, 50=已报, 51=部分成交,
# 52=全部成交, 53=部分撤单 (part filled, remainder cancelled),
# 54=全部撤单, 55/56=已发送 (still in flight).
# ---------------------------------------------------------------------------
WIRE_ORDER_UNREPORTED: Final[int] = 48
WIRE_ORDER_WAIT_REPORT: Final[int] = 49
WIRE_ORDER_REPORTED: Final[int] = 50
WIRE_ORDER_PART_FILLED: Final[int] = 51
WIRE_ORDER_ALL_FILLED: Final[int] = 52
WIRE_ORDER_PART_CANCELLED: Final[int] = 53
WIRE_ORDER_ALL_CANCELLED: Final[int] = 54
WIRE_ORDER_SENDED: Final[int] = 55

#: Order-side codes (xtconstant.STOCK_BUY / STOCK_SELL).
WIRE_SIDE_BUY: Final[int] = 23
WIRE_SIDE_SELL: Final[int] = 24

#: Fixed (limit) price type (xtconstant.FIX_PRICE).
WIRE_PRICE_FIX: Final[int] = 11


class WirePhase(enum.Enum):
    """Pulsar-level phase of a wire order record.

    Channel-private intermediate phases (in flight, already reported) are
    digested inside the adapter; only fill/cancel outcomes surface as
    :class:`~pulsar_contracts.execution.ExecutionEvent`s.  ``UNKNOWN`` maps
    to the unconfirmed path (ERROR event, state unchanged, converge by
    reconciliation) — never to an assumed failure.
    """

    IN_FLIGHT = "in_flight"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"  # full cancel (53 with no fill behaves the same)
    UNKNOWN = "unknown"


_WIRE_PHASE_BY_CODE: Final[dict[int, WirePhase]] = {
    WIRE_ORDER_UNREPORTED: WirePhase.IN_FLIGHT,
    WIRE_ORDER_WAIT_REPORT: WirePhase.IN_FLIGHT,
    WIRE_ORDER_REPORTED: WirePhase.IN_FLIGHT,
    WIRE_ORDER_SENDED: WirePhase.IN_FLIGHT,
    WIRE_ORDER_PART_FILLED: WirePhase.PARTIALLY_FILLED,
    WIRE_ORDER_ALL_FILLED: WirePhase.FILLED,
    WIRE_ORDER_PART_CANCELLED: WirePhase.CANCELLED,
    WIRE_ORDER_ALL_CANCELLED: WirePhase.CANCELLED,
}


def wire_phase(status_code: int) -> WirePhase:
    """Map a miniQMT wire status code onto a :class:`WirePhase`."""
    return _WIRE_PHASE_BY_CODE.get(status_code, WirePhase.UNKNOWN)


def side_code_of(side: str) -> int:
    """Map a Pulsar side value (``buy``/``sell``) onto the wire side code."""
    if side == "buy":
        return WIRE_SIDE_BUY
    if side == "sell":
        return WIRE_SIDE_SELL
    raise ValueError(f"cannot map side {side!r} onto a miniQMT wire side code")


def wire_price_type(price_mode: str) -> int:
    """Map a Pulsar price mode onto the miniQMT wire price-type code.

    First live delivery: limit orders only.  Marketable modes (counter
    price, five-level-IOC) exist on miniQMT but their constant values are
    not wired up yet — refusing loudly beats guessing a code that sends a
    real order with the wrong price type.
    """
    if price_mode == "limit":
        return WIRE_PRICE_FIX
    raise UnsupportedPriceModeError(price_mode)


# ---------------------------------------------------------------------------
# Wire records (clean mirrors of xtquant.xttype values)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class WireOrder:
    """Broker-side order record (mirror of ``xttype.XtOrder``)."""

    wire_order_id: int
    symbol: str
    side_code: int
    price: float
    quantity: int
    filled_quantity: int
    status_code: int
    status_msg: str = ""


@dataclass(frozen=True)
class WireTrade:
    """Broker-side trade (fill) record (mirror of ``xttype.XtTrade``)."""

    trade_id: str
    wire_order_id: int
    symbol: str
    price: float
    quantity: int
    ts_epoch: int


@dataclass(frozen=True)
class WirePosition:
    """Broker-side position record (mirror of ``xttype.XtPosition``)."""

    symbol: str
    quantity: int
    available_quantity: int
    avg_cost: float | None = None


@dataclass(frozen=True)
class WireAsset:
    """Broker-side account asset snapshot (mirror of ``xttype.XtAsset``)."""

    cash: float
    frozen_cash: float
    market_value: float
    total_asset: float


# ---------------------------------------------------------------------------
# Structural protocols the gateway depends on
# ---------------------------------------------------------------------------
@runtime_checkable
class BrokerSession(Protocol):
    """The broker session surface :class:`MiniQMTGateway` needs.

    The account binding is intentionally not part of the protocol: the
    concrete bridge (or test fake) owns it, so the gateway never sees
    account identifiers.
    """

    def connect(self) -> None:
        """Open the session; raise :class:`BrokerUnavailableError` on failure."""
        ...

    def disconnect(self) -> None:
        """Close the session (idempotent)."""
        ...

    def is_connected(self) -> bool:
        """Whether the session currently reports itself connected."""
        ...

    def register_callbacks(self, callbacks: BrokerCallbacks) -> None:
        """Register the push-callback receiver (order/trade/connection)."""
        ...

    def place_order(
        self,
        symbol: str,
        side_code: int,
        quantity: int,
        price_type: int,
        price: float,
    ) -> int:
        """Place an order; return its wire order id.

        Return a negative value for a *confirmed* refusal (the broker API
        rejected the call and no order exists).  Raise
        :class:`BrokerUnavailableError` when the outcome is *unknown* — the
        caller must then treat the order as unconfirmed and converge via
        queries.
        """
        ...

    def cancel_order(self, wire_order_id: int) -> int:
        """Request cancellation; return a non-negative acknowledgement.

        Negative means confirmed refusal (e.g. already filled); raising
        :class:`BrokerUnavailableError` means the cancel outcome is unknown
        and must be reconciled.
        """
        ...

    def query_orders(self) -> list[WireOrder]:
        """All order records of the bound account for this session."""
        ...

    def query_trades(self) -> list[WireTrade]:
        """All trade records of the bound account for this session."""
        ...

    def query_positions(self) -> list[WirePosition]:
        """Current positions of the bound account."""
        ...

    def query_asset(self) -> WireAsset:
        """Current account asset snapshot."""
        ...


@runtime_checkable
class BrokerCallbacks(Protocol):
    """Push callbacks delivered by the broker session."""

    def on_order_push(self, record: WireOrder) -> None:
        """An order record was pushed (progress or terminal state)."""
        ...

    def on_trade_push(self, record: WireTrade) -> None:
        """A trade (fill) record was pushed."""
        ...

    def on_disconnected(self) -> None:
        """The session dropped; no outcome may be assumed until reconciled."""
        ...

    def on_reconnected(self) -> None:
        """The session reconnected; reconcile before resuming."""
        ...
