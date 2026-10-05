"""T+1 account tests: cash flows, availability rolls, hand-checked numbers."""

from __future__ import annotations

import pytest
from pulsar_contracts import Side

from pulsar_exec import BacktestAccount, FeeBreakdown, FeeSchedule, compute_fees


def fees_of(price: float, quantity: int, side: Side) -> FeeBreakdown:
    return compute_fees(
        price=price, quantity=quantity, side=side, schedule=FeeSchedule()
    )


class TestBuyFlow:
    def test_buy_deducts_value_and_fees_and_parks_shares(self):
        # Buy 1000 @ 10.00:
        #   value        = 10,000.00
        #   fees         = 5.10 (commission 5.00 minimum + transfer 0.10)
        #   cash         = 100,000 - 10,005.10 = 89,994.90
        #   avg cost     = 10,005.10 / 1000 = 10.0051 (fees in basis)
        account = BacktestAccount(cash=100_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        assert account.cash == pytest.approx(89_994.90)
        assert account.available_quantity("600519") == 0  # T+1: not sellable today
        position = account.position_views()[0]
        assert position.quantity == 1000
        assert position.available_quantity == 0
        assert position.avg_cost == pytest.approx(10.0051, abs=1e-4)

    def test_overdraft_buy_rejected(self):
        account = BacktestAccount(cash=100.0)
        with pytest.raises(ValueError, match="cash"):
            account.apply_buy("600519", 100, 10.00, fees_of(10.00, 100, Side.BUY))


class TestSellFlow:
    def test_sell_before_roll_rejected(self):
        account = BacktestAccount(cash=100_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        with pytest.raises(ValueError, match="T\\+1"):
            account.apply_sell("600519", 1000, 10.50, fees_of(10.50, 1000, Side.SELL))

    def test_roll_then_sell_books_proceeds(self):
        # Buy 1000 @ 10.00 -> cash 89,994.90, avg cost 10.0051.
        # Roll the day: 1000 shares become available.
        # Sell 600 @ 10.50:
        #   value    = 6,300.00
        #   fees     = commission max(1.89, 5.00) = 5.00
        #              stamp 6,300.00 * 0.0005 = 3.15
        #              transfer 6,300.00 * 0.00001 = 0.063 -> 0.06
        #   proceeds = 6,300.00 - 8.21 = 6,291.79
        #   cash     = 89,994.90 + 6,291.79 = 96,286.69
        account = BacktestAccount(cash=100_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        account.roll_trading_day()
        account.apply_sell("600519", 600, 10.50, fees_of(10.50, 600, Side.SELL))
        assert account.cash == pytest.approx(96_286.69)
        position = account.position_views()[0]
        assert position.quantity == 400
        assert position.available_quantity == 400

    def test_oversell_rejected(self):
        account = BacktestAccount(cash=100_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        account.roll_trading_day()
        with pytest.raises(ValueError, match="available"):
            account.apply_sell("600519", 1500, 10.50, fees_of(10.50, 1500, Side.SELL))

    def test_flat_position_resets_cost_basis(self):
        account = BacktestAccount(cash=100_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        account.roll_trading_day()
        account.apply_sell("600519", 1000, 10.50, fees_of(10.50, 1000, Side.SELL))
        assert account.position_views() == []
        lot = account.lot("600519")
        assert lot.quantity == 0
        assert lot.avg_cost is None


class TestTRoll:
    def test_rolls_accumulate_across_days(self):
        # Day 1: buy 1000 (parked). Day 2: buy 500 more (parked), sell 700
        # of the day-1 shares. Day 3 roll: 800 available (300 + 500).
        account = BacktestAccount(cash=1_000_000.0)
        account.apply_buy("600519", 1000, 10.00, fees_of(10.00, 1000, Side.BUY))
        account.roll_trading_day()
        account.apply_buy("600519", 500, 10.00, fees_of(10.00, 500, Side.BUY))
        account.apply_sell("600519", 700, 10.00, fees_of(10.00, 700, Side.SELL))
        assert account.available_quantity("600519") == 300
        account.roll_trading_day()
        assert account.available_quantity("600519") == 800
        position = account.position_views()[0]
        assert position.quantity == 800
        assert position.available_quantity == 800
