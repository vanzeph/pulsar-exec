"""Test doubles for the live channel: a scriptable in-memory broker.

``FakeBrokerSession`` implements the ``BrokerSession`` protocol exactly as
the xtquant bridge would, keeping an internal broker-side book (orders,
trades, positions, asset) that the tests can inspect and corrupt on
purpose (dropped orders, orphan trades, position drift, silent fills) to
drive the reconciliation paths.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field, replace

from pulsar_contracts import SHANGHAI_TZ
from pulsar_exec.live.protocol import (
    BrokerUnavailableError,
    WireAsset,
    WireOrder,
    WirePosition,
    WireTrade,
    WIRE_ORDER_ALL_CANCELLED,
    WIRE_ORDER_ALL_FILLED,
    WIRE_ORDER_PART_CANCELLED,
    WIRE_ORDER_PART_FILLED,
    WIRE_ORDER_REPORTED,
)

__all__ = ["FakeBrokerSession", "MutableClock", "epoch_of"]


class MutableClock:
    """Callable clock the tests can move (day rollover, timestamps)."""

    def __init__(self, now) -> None:
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, **kwargs) -> None:
        self.now = self.now.replace(**kwargs)


def epoch_of(dt) -> int:
    """Wall-clock Shanghai timestamp of ``dt`` (naive or aware)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SHANGHAI_TZ)
    return int(dt.timestamp())


@dataclass
class _BrokerOrderEntry:
    record: WireOrder
    filled: int = 0


class FakeBrokerSession:
    """In-memory broker: place, cancel, fill, query — plus failure knobs."""

    def __init__(
        self,
        *,
        positions: tuple[WirePosition, ...] = (),
        cash: float = 1_000_000.0,
    ) -> None:
        self._connected = False
        self._callbacks = None
        self._wire_ids = itertools.count(10_000)
        self._orders: dict[int, _BrokerOrderEntry] = {}
        self._trades: dict[str, WireTrade] = {}
        self._positions: dict[str, WirePosition] = {p.symbol: p for p in positions}
        self.asset = WireAsset(cash=cash, frozen_cash=0.0, market_value=0.0, total_asset=cash)

        # failure scripting
        self.unavailable = False
        self.refuse_orders = False
        self.cancel_unconfirmed: set[int] = set()
        self.cancel_refused: set[int] = set()

        # observability
        self.placed_calls: list[tuple[str, int, int, int, float]] = []
        self.cancel_requests: list[int] = []
        self.disconnected_count = 0
        self.reconnected_count = 0

    # -- lifecycle -----------------------------------------------------
    def connect(self) -> None:
        if self.unavailable:
            raise BrokerUnavailableError("connect refused (scripted)")
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def register_callbacks(self, callbacks) -> None:
        self._callbacks = callbacks

    def _require_available(self) -> None:
        if self.unavailable or not self._connected:
            raise BrokerUnavailableError("broker session not available (scripted)")

    def _push_order(self, wire_order_id: int) -> None:
        if self._callbacks is not None:
            self._callbacks.on_order_push(self._orders[wire_order_id].record)

    def _push_trade(self, trade: WireTrade) -> None:
        if self._callbacks is not None:
            self._callbacks.on_trade_push(trade)

    # -- trading ---------------------------------------------------------
    def place_order(
        self,
        symbol: str,
        side_code: int,
        quantity: int,
        price_type: int,
        price: float,
    ) -> int:
        self._require_available()
        self.placed_calls.append((symbol, side_code, quantity, price_type, price))
        if self.refuse_orders:
            return -1
        wire_order_id = next(self._wire_ids)
        self._orders[wire_order_id] = _BrokerOrderEntry(
            record=WireOrder(
                wire_order_id=wire_order_id,
                symbol=symbol,
                side_code=side_code,
                price=price,
                quantity=quantity,
                filled_quantity=0,
                status_code=WIRE_ORDER_REPORTED,
                status_msg="已报",
            )
        )
        self._push_order(wire_order_id)
        return wire_order_id

    def cancel_order(self, wire_order_id: int) -> int:
        self._require_available()
        self.cancel_requests.append(wire_order_id)
        if wire_order_id in self.cancel_unconfirmed:
            raise BrokerUnavailableError("cancel outcome unknown (scripted)")
        entry = self._orders.get(wire_order_id)
        if entry is None or wire_order_id in self.cancel_refused:
            return -1
        code = WIRE_ORDER_PART_CANCELLED if entry.filled > 0 else WIRE_ORDER_ALL_CANCELLED
        self._set_status(wire_order_id, code, "撤单")
        return wire_order_id

    def exchange_fill(
        self,
        wire_order_id: int,
        quantity: int,
        price: float,
        ts_epoch: int,
        *,
        silent: bool = False,
    ) -> WireTrade:
        """Book an exchange execution and (unless silent) push it."""
        entry = self._orders[wire_order_id]
        entry.filled += quantity
        code = (
            WIRE_ORDER_ALL_FILLED
            if entry.filled >= entry.record.quantity
            else WIRE_ORDER_PART_FILLED
        )
        trade_id = f"XT{len(self._trades) + 1:06d}"
        trade = WireTrade(
            trade_id=trade_id,
            wire_order_id=wire_order_id,
            symbol=entry.record.symbol,
            price=price,
            quantity=quantity,
            ts_epoch=ts_epoch,
        )
        self._trades[trade_id] = trade
        if not silent:
            self._push_trade(trade)
        self._set_status(wire_order_id, code, "成交")
        # the broker-side position book moves with every execution
        symbol = entry.record.symbol
        delta = quantity if entry.record.side_code == 23 else -quantity
        held = self._positions.get(symbol)
        new_quantity = (held.quantity if held else 0) + delta
        self._positions[symbol] = WirePosition(
            symbol=symbol,
            quantity=new_quantity,
            available_quantity=max(0, new_quantity),
            avg_cost=(held.avg_cost if held else None) or price,
        )
        return trade

    def _set_status(self, wire_order_id: int, code: int, msg: str) -> None:
        entry = self._orders[wire_order_id]
        entry.record = replace(
            entry.record,
            status_code=code,
            status_msg=msg,
            filled_quantity=entry.filled,
        )
        self._push_order(wire_order_id)

    # -- corruption helpers for reconciliation tests -----------------------
    def drop_order(self, wire_order_id: int) -> None:
        """Make the order vanish from the broker book (never received)."""
        self._orders.pop(wire_order_id, None)

    def inject_orphan_order(self, wire_order_id: int, symbol: str, quantity: int) -> None:
        """A broker order this run never submitted (manual activity)."""
        self._orders[wire_order_id] = _BrokerOrderEntry(
            record=WireOrder(
                wire_order_id=wire_order_id,
                symbol=symbol,
                side_code=23,
                price=10.0,
                quantity=quantity,
                filled_quantity=0,
                status_code=WIRE_ORDER_REPORTED,
                status_msg="manual",
            )
        )

    def inject_orphan_trade(self, wire_order_id: int, symbol: str, quantity: int, price: float, ts_epoch: int) -> WireTrade:
        """A broker trade on a wire id this run does not know."""
        trade_id = f"XTORPH{len(self._trades) + 1:06d}"
        trade = WireTrade(
            trade_id=trade_id,
            wire_order_id=wire_order_id,
            symbol=symbol,
            price=price,
            quantity=quantity,
            ts_epoch=ts_epoch,
        )
        self._trades[trade_id] = trade
        return trade

    def drift_position(self, symbol: str, quantity: int) -> None:
        """Broker-side position change with no trade record (drift)."""
        self._positions[symbol] = WirePosition(
            symbol=symbol, quantity=quantity, available_quantity=quantity
        )

    def set_position(self, symbol: str, quantity: int) -> None:
        self.drift_position(symbol, quantity)

    # -- connection choreography ------------------------------------------
    def simulate_drop(self) -> None:
        self._connected = False
        self.disconnected_count += 1
        if self._callbacks is not None:
            self._callbacks.on_disconnected()

    def simulate_resume(self) -> None:
        self._connected = True
        self.reconnected_count += 1
        if self._callbacks is not None:
            self._callbacks.on_reconnected()

    # -- queries --------------------------------------------------------------
    def query_orders(self) -> list[WireOrder]:
        self._require_available()
        return [entry.record for entry in self._orders.values()]

    def query_trades(self) -> list[WireTrade]:
        self._require_available()
        return list(self._trades.values())

    def query_positions(self) -> list[WirePosition]:
        self._require_available()
        return list(self._positions.values())

    def query_asset(self) -> WireAsset:
        self._require_available()
        return self.asset

    def wire_id_of_last_placed(self) -> int:
        return max(self._orders) if self._orders else -1
