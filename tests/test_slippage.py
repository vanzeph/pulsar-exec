"""Slippage-model tests with hand-derived arithmetic."""

from __future__ import annotations

import pytest
from pulsar_contracts import Side

from pulsar_exec import SlippageModel, apply_slippage


def slipped(price: float, side: Side, model: SlippageModel, share: float = 0.0):
    return apply_slippage(
        reference_price=price, side=side, model=model, volume_share=share
    )


class TestFixedBps:
    def test_buy_slips_up(self):
        # 10 bps on 10.00: 10.00 * (1 + 0.0010) = 10.01
        model = SlippageModel(fixed_bps=10.0)
        assert slipped(10.00, Side.BUY, model) == pytest.approx(10.01)

    def test_sell_slips_down(self):
        # 10 bps on 10.00: 10.00 * (1 - 0.0010) = 9.99
        model = SlippageModel(fixed_bps=10.0)
        assert slipped(10.00, Side.SELL, model) == pytest.approx(9.99)

    def test_default_is_conservative_five_bps(self):
        # Default model: 5 bps. 100.00 * 1.0005 = 100.05 exactly.
        model = SlippageModel()
        assert model.fixed_bps == 5.0
        assert model.volume_impact_coeff == 0.0
        assert slipped(100.00, Side.BUY, model) == pytest.approx(100.05)
        assert slipped(100.00, Side.SELL, model) == pytest.approx(99.95)

    def test_zero_model_returns_reference(self):
        model = SlippageModel(fixed_bps=0.0)
        assert slipped(12.34, Side.BUY, model) == pytest.approx(12.34)
        assert slipped(12.34, Side.SELL, model) == pytest.approx(12.34)


class TestVolumeImpact:
    def test_impact_term_scales_with_share(self):
        # coeff 0.5, share 0.02 (fill takes 2% of bar volume), bps 0:
        #   buy: 10.00 * (1 + 0.5 * 0.02) = 10.00 * 1.01 = 10.10
        model = SlippageModel(fixed_bps=0.0, volume_impact_coeff=0.5)
        assert slipped(10.00, Side.BUY, model, share=0.02) == pytest.approx(10.10)

    def test_impact_and_bps_combine(self):
        # bps 5, coeff 0.1, share 0.5, sell 20.00:
        #   20.00 * (1 - 0.0005 - 0.1*0.5) = 20.00 * 0.9495 = 18.99
        model = SlippageModel(fixed_bps=5.0, volume_impact_coeff=0.1)
        assert slipped(20.00, Side.SELL, model, share=0.5) == pytest.approx(18.99)

    def test_full_bar_consumption_is_worst_case(self):
        # share is clamped to 1: coeff 0.2, bps 0, buy 5.00:
        #   5.00 * (1 + 0.2) = 6.00; share 1.5 must clamp to the same
        model = SlippageModel(fixed_bps=0.0, volume_impact_coeff=0.2)
        worst = slipped(5.00, Side.BUY, model, share=1.0)
        clamped = slipped(5.00, Side.BUY, model, share=1.5)
        assert worst == clamped == pytest.approx(6.00)


class TestRoundingAndFloor:
    def test_rounds_to_cent_half_up(self):
        # 3 bps on 9.99 buy: 9.99 * 1.0003 = 9.992997 -> 9.99
        model = SlippageModel(fixed_bps=3.0)
        assert slipped(9.99, Side.BUY, model) == pytest.approx(9.99)
        # 7 bps on 9.99 sell: 9.99 * 0.9993 = 9.983007 -> 9.98
        model = SlippageModel(fixed_bps=7.0)
        assert slipped(9.99, Side.SELL, model) == pytest.approx(9.98)

    def test_price_floored_at_one_cent(self):
        # 9900 bps sell on 0.50: 0.50 * 0.01 = 0.005 -> floored to 0.01
        model = SlippageModel(fixed_bps=9_900.0)
        assert slipped(0.50, Side.SELL, model) == pytest.approx(0.01)

    def test_non_positive_price_rejected(self):
        with pytest.raises(ValueError):
            slipped(0.0, Side.BUY, SlippageModel())
