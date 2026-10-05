"""Per-fill fee computation: commission, stamp duty, transfer fee.

Fees are booked per fill (逐笔计提) with the rates of the run's
:class:`~pulsar_exec.config.FeeSchedule`. Every component is computed in
exact decimal arithmetic on the *slipped* fill amount and rounded to the
cent (ROUND_HALF_UP) before being reported on the
:class:`~pulsar_contracts.execution.Fill` — the rounding discipline real
broker statements follow, and the one the golden hand-checks rely on.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

from pulsar_contracts import Side

from .config import CENT, FeeSchedule

__all__ = ["FeeBreakdown", "compute_fees"]


@dataclass(frozen=True)
class FeeBreakdown:
    """The three A-share fee components of one fill, in CNY."""

    commission: float
    stamp_duty: float
    transfer_fee: float

    @property
    def total(self) -> float:
        """Sum of all components."""
        return round(self.commission + self.stamp_duty + self.transfer_fee, 2)


def _to_cent(value: Decimal) -> float:
    """Round a Decimal money value to the cent, ROUND_HALF_UP."""
    return float(value.quantize(CENT, rounding=ROUND_HALF_UP))


def compute_fees(
    *,
    price: float,
    quantity: int,
    side: Side,
    schedule: FeeSchedule,
) -> FeeBreakdown:
    """Compute the fees of one execution of ``quantity`` shares at ``price``.

    * commission: ``max(amount * commission_rate, min_commission)`` — both
      directions, subject to the per-trade minimum;
    * stamp duty: ``amount * stamp_duty_rate`` — **sell only**;
    * transfer fee: ``amount * transfer_fee_rate`` — both directions.

    ``amount = price * quantity`` computed exactly in Decimal; each
    component rounded to the cent independently.
    """
    if price <= 0:
        raise ValueError("price must be positive")
    if quantity <= 0:
        raise ValueError("quantity must be positive")

    amount = Decimal(str(price)) * Decimal(str(quantity))

    raw_commission = amount * Decimal(str(schedule.commission_rate))
    commission = max(raw_commission, Decimal(str(schedule.min_commission)))

    stamp_duty = (
        amount * Decimal(str(schedule.stamp_duty_rate))
        if side is Side.SELL
        else Decimal("0")
    )

    transfer_fee = amount * Decimal(str(schedule.transfer_fee_rate))

    return FeeBreakdown(
        commission=_to_cent(commission),
        stamp_duty=_to_cent(stamp_duty),
        transfer_fee=_to_cent(transfer_fee),
    )
