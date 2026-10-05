"""Simulated cash account with T+1 sellable-position bookkeeping.

The account models the A-share settlement constraint the Pulsar execution
design mandates:

* **cash** — CNY ledger; buys deduct value + fees, sells credit value − fees;
* **positions** — per symbol: total quantity, T+1 *available* quantity and
  fee-inclusive average cost. Shares bought on trading day *D* sit in a
  "bought today" bucket and only become sellable when the day rolls
  (:meth:`BacktestAccount.roll_trading_day`), i.e. on day *D+1*;
* **fees** — accrued per fill by the venue via :mod:`pulsar_exec.fees` and
  folded into cash and cost basis here.

Selling is restricted to the available quantity (no shorting, no T+0); the
venue rejects or clips orders before they ever reach this ledger.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pulsar_contracts import Position

from .fees import FeeBreakdown

__all__ = ["SymbolLot", "BacktestAccount"]


@dataclass
class SymbolLot:
    """Mutable book of one symbol's holdings under T+1.

    ``bought_today`` counts shares purchased on the current trading day;
    they are excluded from ``available`` until the trading day rolls.
    """

    quantity: int = 0
    available: int = 0
    bought_today: int = 0
    avg_cost: float | None = None


@dataclass
class BacktestAccount:
    """Cash + T+1 position ledger of one backtest run.

    The venue is the only intended caller; every mutation corresponds to
    one confirmed fill (or the day rollover), so the ledger can be
    replayed and audited fill by fill.
    """

    cash: float
    positions: dict[str, SymbolLot] = field(default_factory=dict)

    def lot(self, symbol: str) -> SymbolLot:
        """Return (creating if needed) the lot of ``symbol``."""
        return self.positions.setdefault(symbol, SymbolLot())

    def available_quantity(self, symbol: str) -> int:
        """Shares of ``symbol`` sellable right now (T+1 respected)."""
        lot = self.positions.get(symbol)
        return lot.available if lot is not None else 0

    def apply_buy(
        self, symbol: str, quantity: int, price: float, fees: FeeBreakdown
    ) -> None:
        """Book a confirmed buy: pay value + fees, park shares for T+1."""
        if quantity <= 0:
            raise ValueError("buy quantity must be positive")
        lot = self.lot(symbol)
        value = round(price * quantity, 2)
        total_cost = round(value + fees.total, 2)
        if total_cost > self.cash + 0.005:
            raise ValueError(
                f"buy of {quantity} x {symbol} @ {price} costs {total_cost} "
                f"but only {self.cash:.2f} cash is available"
            )
        previous_value = (lot.avg_cost or 0.0) * lot.quantity
        self.cash = round(self.cash - total_cost, 2)
        lot.quantity += quantity
        lot.bought_today += quantity
        lot.avg_cost = round((previous_value + total_cost) / lot.quantity, 4)

    def apply_sell(
        self, symbol: str, quantity: int, price: float, fees: FeeBreakdown
    ) -> None:
        """Book a confirmed sell: deliver available shares, receive value − fees."""
        if quantity <= 0:
            raise ValueError("sell quantity must be positive")
        lot = self.lot(symbol)
        if quantity > lot.available:
            raise ValueError(
                f"sell of {quantity} x {symbol} exceeds available "
                f"{lot.available} (T+1 constraint)"
            )
        proceeds = round(price * quantity - fees.total, 2)
        self.cash = round(self.cash + proceeds, 2)
        lot.quantity -= quantity
        lot.available -= quantity
        if lot.quantity == 0:
            # flat again: reset cost basis so a new cycle starts clean
            lot.avg_cost = None

    def roll_trading_day(self) -> None:
        """Close the trading day: today's buys become sellable (T+1 roll)."""
        for lot in self.positions.values():
            lot.available += lot.bought_today
            lot.bought_today = 0

    def position_views(self) -> list[Position]:
        """Contract-level position snapshots, one per held symbol."""
        views = [
            Position(
                symbol=symbol,
                quantity=lot.quantity,
                available_quantity=lot.available,
                avg_cost=lot.avg_cost,
            )
            for symbol, lot in sorted(self.positions.items())
            if lot.quantity > 0
        ]
        return views
