"""Slippage model of the backtest venue: fixed bps + optional volume impact.

The venue matches orders against bar prices; simulated fills are then
penalized (buys shifted up, sells shifted down) by::

    reference_price * (fixed_bps / 1e4 + volume_impact_coeff * share)

with ``share`` the fraction of the bar's volume the fill consumes. The
result is rounded to the cent (ROUND_HALF_UP) and floored at one cent so
it stays a representable A-share price. Parameters come from the run's
:class:`~pulsar_exec.config.SlippageModel`; the default (5 bps fixed, no
impact term) is deliberately conservative.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from pulsar_contracts import Side

from .config import CENT, SlippageModel

__all__ = ["apply_slippage"]


def apply_slippage(
    *,
    reference_price: float,
    side: Side,
    model: SlippageModel,
    volume_share: float = 0.0,
) -> float:
    """Return the slipped fill price for one execution.

    ``volume_share`` is ``fill_quantity / bar_volume`` clamped to
    ``[0, 1]``; it drives the optional volume-impact term. Set
    ``fixed_bps=0`` and ``volume_impact_coeff=0`` to match at the raw
    bar price.
    """
    if reference_price <= 0:
        raise ValueError("reference_price must be positive")

    share = min(max(volume_share, 0.0), 1.0)
    penalty = (
        Decimal(str(model.fixed_bps)) / Decimal(10_000)
        + Decimal(str(model.volume_impact_coeff)) * Decimal(str(share))
    )

    price = Decimal(str(reference_price))
    slipped = price * (Decimal(1) + penalty) if side is Side.BUY else price * (
        Decimal(1) - penalty
    )

    slipped = slipped.quantize(CENT, rounding=ROUND_HALF_UP)
    slipped = max(slipped, CENT)  # a price can never fall below one cent
    return float(slipped)
