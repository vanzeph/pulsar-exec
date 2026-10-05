"""Runtime bridge from the local protocol to the real xtquant SDK.

``xtquant`` ships with the miniQMT terminal and cannot be installed on
machines without it.  This module is therefore the **only** place in the
package that references ``xtquant`` at all, and it does so *dynamically*
(``importlib``, inside the factory) — importing :mod:`pulsar_exec` never
requires the SDK, and the adapter stays fully testable through the local
:class:`~pulsar_exec.live.protocol.BrokerSession` protocol.

Credentials policy: the code only ever sees the *names* of environment
variables (``PULSAR_MINIQMT_ACCOUNT_ID`` by default, plus optional
``PULSAR_MINIQMT_USERDATA`` / ``PULSAR_MINIQMT_SESSION_ID``); values come
from the environment (or an injected mapping in tests).  No account id,
path or token literal belongs in source.

Known first-delivery limitations (to be exercised in the broker simulation
drill): callback error codes (``on_order_error`` / ``on_cancel_error``)
are surfaced as unconfirmed (UNKNOWN-phase) records rather than decoded;
marketable price modes are refused by :func:`wire_price_type` upstream.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any

from .protocol import (
    BrokerCallbacks,
    BrokerSession,
    BrokerUnavailableError,
    MiniQMTUnavailableError,
    WireAsset,
    WireOrder,
    WirePosition,
    WireTrade,
)

__all__ = [
    "DEFAULT_ACCOUNT_ENV",
    "DEFAULT_USERDATA_ENV",
    "DEFAULT_SESSION_ID_ENV",
    "open_broker_session",
]

DEFAULT_ACCOUNT_ENV = "PULSAR_MINIQMT_ACCOUNT_ID"
DEFAULT_USERDATA_ENV = "PULSAR_MINIQMT_USERDATA"
DEFAULT_SESSION_ID_ENV = "PULSAR_MINIQMT_SESSION_ID"

#: Wire status code used by the bridge for records it cannot decode; maps
#: to the UNKNOWN phase (unconfirmed) inside the gateway.
_UNDECIDED_STATUS = -1


def _import_xtquant() -> tuple[Any, Any]:
    """Dynamically load ``(xttrader, xttype)``; raise a clear error if absent."""
    try:
        xttrader = importlib.import_module("xtquant.xttrader")
        xttype = importlib.import_module("xtquant.xttype")
    except ImportError as exc:  # pragma: no cover - exercised only off-terminals
        raise MiniQMTUnavailableError(
            "the xtquant package is not importable on this machine: the miniQMT "
            "terminal must be installed (its xtquant directory on PYTHONPATH) "
            "before a live session can be opened"
        ) from exc
    return xttrader, xttype


def _wire_order(record: Any) -> WireOrder:
    # ``m_nOrderType`` carries the side on order records; ``side_code`` is
    # informational only — the gateway takes sides from local orders.
    side = getattr(record, "m_nOrderType", None)
    if side is None:
        side = getattr(record, "m_nOffsetFlag", 0)
    return WireOrder(
        wire_order_id=int(record.m_nOrderId),
        symbol=str(record.m_strStockCode),
        side_code=int(side if side is not None else 0),
        price=float(record.m_dPrice),
        quantity=int(record.m_nVolume),
        filled_quantity=int(record.m_nTradedVolume),
        status_code=int(record.m_nOrderStatus),
        status_msg=str(getattr(record, "m_strStatusMsg", "") or ""),
    )


def _wire_trade(record: Any) -> WireTrade:
    return WireTrade(
        trade_id=str(record.m_strTradedId),
        wire_order_id=int(record.m_nOrderId),
        symbol=str(record.m_strStockCode),
        price=float(record.m_dPrice),
        quantity=int(record.m_nTradedVolume),
        ts_epoch=int(record.m_nTradedTime),
    )


def _wire_position(record: Any) -> WirePosition:
    can_use = int(getattr(record, "m_nCanUseVolume", record.m_nVolume))
    avg_cost = getattr(record, "dPositionAvgPrice", None)
    if avg_cost is None:
        avg_cost = getattr(record, "m_dPositionAvgPrice", None)
    return WirePosition(
        symbol=str(record.m_strStockCode),
        quantity=int(record.m_nVolume),
        available_quantity=can_use,
        avg_cost=float(avg_cost) if avg_cost else None,
    )


def _wire_asset(record: Any) -> WireAsset:
    return WireAsset(
        cash=float(record.m_dCash),
        frozen_cash=float(record.m_dFrozenCash),
        market_value=float(record.m_dMarketValue),
        total_asset=float(record.m_dTotalAsset),
    )


class _CallbackBridge:
    """Adapts xttrader's callback object onto :class:`BrokerCallbacks`."""

    def __init__(self, sink: BrokerCallbacks) -> None:
        self._sink = sink

    def on_disconnected(self) -> None:
        self._sink.on_disconnected()

    def on_stock_order(self, order: Any, err: int = 0) -> None:
        record = _wire_order(order)
        if err != 0:  # undecodable report -> unconfirmed, never assumed
            record = WireOrder(
                wire_order_id=record.wire_order_id,
                symbol=record.symbol,
                side_code=record.side_code,
                price=record.price,
                quantity=record.quantity,
                filled_quantity=record.filled_quantity,
                status_code=_UNDECIDED_STATUS,
                status_msg=f"order callback error code {err}",
            )
        self._sink.on_order_push(record)

    def on_stock_trade(self, trade: Any, err: int = 0) -> None:
        if err != 0:
            return  # no usable trade payload; convergence via queries
        self._sink.on_trade_push(_wire_trade(trade))

    def on_order_error(self, order_error: Any, err: int = 0) -> None:
        order_id = int(getattr(order_error, "m_nOrderId", 0) or 0)
        record = WireOrder(
            wire_order_id=order_id,
            symbol=str(getattr(order_error, "m_strStockCode", "") or ""),
            side_code=0,
            price=0.0,
            quantity=0,
            filled_quantity=0,
            status_code=_UNDECIDED_STATUS,
            status_msg=f"order error callback: {err}",
        )
        self._sink.on_order_push(record)

    def on_cancel_error(self, order_action: Any, err: int = 0) -> None:
        order_id = int(getattr(order_action, "m_nOrderId", 0) or 0)
        record = WireOrder(
            wire_order_id=order_id,
            symbol="",
            side_code=0,
            price=0.0,
            quantity=0,
            filled_quantity=0,
            status_code=_UNDECIDED_STATUS,
            status_msg=f"cancel error callback: {err}",
        )
        self._sink.on_order_push(record)

    def on_account_status(self, status: Any, err: int = 0) -> None:
        return  # account-status codes are not order outcomes; ignored here


class _XtQuantSession:
    """``BrokerSession`` implementation wrapping one ``XtTrader`` instance."""

    def __init__(
        self,
        trader: Any,
        account: Any,
        *,
        strategy_name: str,
    ) -> None:
        self._trader = trader
        self._account = account
        self._strategy_name = strategy_name
        self._callbacks: BrokerCallbacks | None = None
        self._connected = False

    # -- lifecycle ----------------------------------------------------------
    def connect(self) -> None:
        if not bool(self._trader.connect()):
            raise BrokerUnavailableError("miniQMT XtTrader.connect() failed")
        self._connected = True

    def disconnect(self) -> None:
        try:
            self._trader.stop()
        finally:
            self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def register_callbacks(self, callbacks: BrokerCallbacks) -> None:
        self._callbacks = callbacks
        self._trader.subscribe(_CallbackBridge(callbacks))

    # -- trading --------------------------------------------------------------
    def place_order(
        self,
        symbol: str,
        side_code: int,
        quantity: int,
        price_type: int,
        price: float,
    ) -> int:
        return int(
            self._trader.order_stock(
                self._account,
                symbol,
                side_code,
                quantity,
                price_type,
                price,
                self._strategy_name,
                "",
            )
        )

    def cancel_order(self, wire_order_id: int) -> int:
        return int(self._trader.cancel_order_stock_async(self._account, wire_order_id))

    # -- queries ----------------------------------------------------------------
    def query_orders(self) -> list[WireOrder]:
        records = self._trader.query_stock_orders(self._account) or []
        return [_wire_order(record) for record in records]

    def query_trades(self) -> list[WireTrade]:
        records = self._trader.query_stock_trades(self._account) or []
        return [_wire_trade(record) for record in records]

    def query_positions(self) -> list[WirePosition]:
        records = self._trader.query_stock_positions(self._account) or []
        return [_wire_position(record) for record in records]

    def query_asset(self) -> WireAsset:
        record = self._trader.query_stock_asset(self._account)
        if record is None:
            raise BrokerUnavailableError("query_stock_asset returned nothing")
        return _wire_asset(record)


def open_broker_session(
    *,
    userdata_path: str | None = None,
    session_id: str | None = None,
    account_env: str = DEFAULT_ACCOUNT_ENV,
    userdata_env: str = DEFAULT_USERDATA_ENV,
    session_id_env: str = DEFAULT_SESSION_ID_ENV,
    env: Mapping[str, str] | None = None,
    strategy_name: str = "pulsar",
) -> BrokerSession:
    """Open a live miniQMT broker session (all inputs via environment names).

    Reads the account id from ``env[account_env]`` (default
    ``PULSAR_MINIQMT_ACCOUNT_ID``; ``os.environ`` when ``env`` is omitted)
    and the miniQMT userdata directory from ``userdata_path`` or
    ``env[userdata_env]``.  Raises :class:`MiniQMTUnavailableError` when
    xtquant/the terminal is absent.
    """
    import os

    source = os.environ if env is None else env

    account_id = source.get(account_env)
    if not account_id:
        raise ValueError(
            f"live account id missing: environment variable {account_env} must "
            f"hold the miniQMT account id (credentials only ever travel "
            f"through the environment)"
        )

    resolved_userdata = userdata_path or source.get(userdata_env)
    if not resolved_userdata:
        raise ValueError(
            f"miniQMT userdata directory missing: pass userdata_path or set "
            f"{userdata_env}"
        )
    resolved_session = session_id or source.get(session_id_env) or "pulsar"

    xttrader, xttype = _import_xtquant()
    trader = xttrader.XtQuantTrader(resolved_userdata, resolved_session)
    account = xttype.XtAccount(account_id)
    return _XtQuantSession(trader, account, strategy_name=strategy_name)
