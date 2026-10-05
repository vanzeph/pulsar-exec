"""Fee-model tests: every number hand-derived and cross-checked.

All cases use the default :class:`~pulsar_exec.config.FeeSchedule`
(commission 0.0003 with a 5.00 CNY minimum, stamp duty 0.0005 on sells,
transfer fee 0.00001 both ways) unless stated otherwise. Each test
carries the manual arithmetic in its comments — the golden acceptance
criterion "费用逐笔与手工核对一致" is checked here at the component level
and end-to-end in ``test_backtest_venue.py``.
"""

from __future__ import annotations

import pytest
from pulsar_contracts import Side

from pulsar_exec import FeeSchedule, compute_fees


def buy(price: float, quantity: int, schedule: FeeSchedule | None = None):
    return compute_fees(
        price=price,
        quantity=quantity,
        side=Side.BUY,
        schedule=schedule or FeeSchedule(),
    )


def sell(price: float, quantity: int, schedule: FeeSchedule | None = None):
    return compute_fees(
        price=price,
        quantity=quantity,
        side=Side.SELL,
        schedule=schedule or FeeSchedule(),
    )


class TestCommission:
    def test_small_buy_pays_the_minimum(self):
        # Buy 1000 @ 10.00 -> amount = 10,000.00 CNY
        #   raw commission = 10,000.00 * 0.0003 = 3.00
        #   3.00 < 5.00 minimum -> commission = 5.00
        #   transfer fee      = 10,000.00 * 0.00001 = 0.10
        #   stamp duty        = 0 (buy pays none)
        fees = buy(10.00, 1000)
        assert fees.commission == pytest.approx(5.00)
        assert fees.stamp_duty == pytest.approx(0.0)
        assert fees.transfer_fee == pytest.approx(0.10)
        assert fees.total == pytest.approx(5.10)

    def test_large_sell_pays_rate_commission(self):
        # Sell 2000 @ 20.35 -> amount = 40,700.00 CNY
        #   raw commission = 40,700.00 * 0.0003 = 12.21 (above 5.00 minimum)
        #   stamp duty     = 40,700.00 * 0.0005 = 20.35 (sell only)
        #   transfer fee   = 40,700.00 * 0.00001 = 0.407 -> 0.41 (HALF_UP)
        fees = sell(20.35, 2000)
        assert fees.commission == pytest.approx(12.21)
        assert fees.stamp_duty == pytest.approx(20.35)
        assert fees.transfer_fee == pytest.approx(0.41)
        assert fees.total == pytest.approx(32.97)

    def test_minimum_applies_to_sells_too(self):
        # Sell 100 @ 10.00 -> amount = 1,000.00
        #   raw commission = 0.30 < 5.00 -> 5.00
        #   stamp = 1,000.00 * 0.0005 = 0.50
        #   transfer = 1,000.00 * 0.00001 = 0.01
        fees = sell(10.00, 100)
        assert fees.commission == pytest.approx(5.00)
        assert fees.stamp_duty == pytest.approx(0.50)
        assert fees.transfer_fee == pytest.approx(0.01)


class TestStampDutyDirection:
    def test_buy_never_pays_stamp_duty(self):
        # Buy 3000 @ 33.33 -> amount = 99,990.00; stamp duty = 0 by rule.
        fees = buy(33.33, 3000)
        assert fees.stamp_duty == 0.0

    def test_sell_stamp_duty_exact(self):
        # Sell 3000 @ 33.33 -> amount = 99,990.00
        #   stamp = 99,990.00 * 0.0005 = 49.995 -> HALF_UP -> 50.00
        fees = sell(33.33, 3000)
        assert fees.stamp_duty == pytest.approx(50.00)


class TestRounding:
    def test_transfer_fee_rounds_half_up_not_bankers(self):
        # Sell 1050 @ 10.00 -> amount = 10,500.00
        #   transfer = 10,500.00 * 0.00001 = 0.105
        #   ROUND_HALF_UP -> 0.11 (banker's rounding would give 0.10;
        #   the venue must follow the statement-style HALF_UP discipline)
        fees = sell(10.00, 1050)
        assert fees.transfer_fee == pytest.approx(0.11)
        #   commission = max(10,500 * 0.0003 = 3.15, 5.00) = 5.00
        #   stamp     = 10,500 * 0.0005 = 5.25
        assert fees.commission == pytest.approx(5.00)
        assert fees.stamp_duty == pytest.approx(5.25)
        assert fees.total == pytest.approx(10.36)

    def test_component_rounding_is_per_component(self):
        # Buy 700 @ 8.89 -> amount = 6,223.00
        #   raw commission = 1.8669 -> below minimum -> 5.00
        #   transfer = 0.06223 -> 0.06
        #   total = 5.06 (sum of rounded components)
        fees = buy(8.89, 700)
        assert fees.commission == pytest.approx(5.00)
        assert fees.transfer_fee == pytest.approx(0.06)
        assert fees.total == pytest.approx(5.06)


class TestConfiguredRates:
    def test_zero_schedule_yields_zero_fees(self):
        schedule = FeeSchedule(
            commission_rate=0.0,
            min_commission=0.0,
            stamp_duty_rate=0.0,
            transfer_fee_rate=0.0,
        )
        assert buy(10.00, 1000, schedule).total == 0.0
        assert sell(10.00, 1000, schedule).total == 0.0

    def test_custom_rates_flow_through(self):
        # Buy 1000 @ 10.00 with commission 0.001, min 0, transfer 0.00002:
        #   commission = 10,000.00 * 0.001 = 10.00
        #   transfer   = 10,000.00 * 0.00002 = 0.20
        schedule = FeeSchedule(
            commission_rate=0.001,
            min_commission=0.0,
            stamp_duty_rate=0.001,
            transfer_fee_rate=0.00002,
        )
        fees = buy(10.00, 1000, schedule)
        assert fees.commission == pytest.approx(10.00)
        assert fees.transfer_fee == pytest.approx(0.20)
        # sell additionally pays stamp: 10,000.00 * 0.001 = 10.00
        fees = sell(10.00, 1000, schedule)
        assert fees.stamp_duty == pytest.approx(10.00)
        assert fees.total == pytest.approx(20.20)


class TestValidation:
    def test_non_positive_inputs_rejected(self):
        with pytest.raises(ValueError):
            buy(0.0, 100)
        with pytest.raises(ValueError):
            buy(10.0, 0)

    def test_negative_rates_rejected_at_config(self):
        with pytest.raises(ValueError):
            FeeSchedule(commission_rate=-0.001)
