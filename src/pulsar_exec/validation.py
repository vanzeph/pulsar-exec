"""Venue-side pre-trade validation shared by the local matching channels.

The Pulsar execution design places board-lot rounding, funding checks,
T+1 availability and price-limit band validation at the venue/gateway —
never in the strategy. Both local matching channels (the bar-driven
:class:`~pulsar_exec.venue.BacktestVenue` and the snapshot-driven
:class:`~pulsar_exec.paper.PaperBroker`) enforce exactly the same rules
through this module, so an intent is either tradable on both channels or
rejected by both: channel consistency starts before the first event is
ever emitted.

The two fill-time helpers live here as well:

* :func:`affordable_quantity` — the largest quantity payable with current
  cash at a given price (fee-aware, including the commission minimum);
* :func:`validate_intent` — the full pre-trade check returning ``None``
  when the intent may rest on the book, or a human-readable rejection
  reason.
"""

from __future__ import annotations

from pulsar_contracts import (
    Instrument,
    InstrumentStatus,
    OrderIntent,
    Side,
    PriceMode,
)

from .account import BacktestAccount
from .config import FeeSchedule, MatchingRules
from .fees import compute_fees
from .price_limit import limit_prices

__all__ = ["validate_intent", "affordable_quantity"]


def affordable_quantity(
    cash: float, price: float, desired: int, schedule: FeeSchedule
) -> int:
    """Largest quantity ≤ ``desired`` payable with ``cash`` at ``price``.

    Accounts for the commission minimum via the actual fee function; the
    analytic starting point leaves at most a couple of correction steps,
    so the decrement loop is bounded and tiny.
    """
    combined = schedule.commission_rate + schedule.transfer_fee_rate

    def cost(qty: int) -> float:
        fees = compute_fees(
            price=price, quantity=qty, side=Side.BUY, schedule=schedule
        )
        return round(price * qty + fees.total, 2)

    candidate = int(cash / (price * (1 + combined)))
    if schedule.min_commission > 0:
        by_minimum = int(
            (cash - schedule.min_commission) / (price * (1 + schedule.transfer_fee_rate))
        )
        candidate = max(candidate, by_minimum)
    qty = max(0, min(desired, candidate))
    while qty > 0 and cost(qty) > cash + 0.005:
        qty -= 1
    return qty


def validate_intent(
    intent: OrderIntent,
    *,
    account: BacktestAccount,
    instrument: Instrument,
    matching: MatchingRules,
    fees: FeeSchedule,
    reference_price: float | None,
    prev_close: float | None,
) -> str | None:
    """Channel-agnostic pre-trade validation; ``None`` means accepted.

    ``reference_price`` is the price the channel uses to estimate the
    funds a marketable buy needs (the intent's limit price or the last
    observed market price); ``prev_close`` drives the day's price-limit
    band and is ``None`` before the channel has seen a session close.
    """
    if instrument.status is InstrumentStatus.SUSPENDED:
        return f"{intent.symbol} is suspended (停牌)"

    if intent.side is Side.BUY:
        lot = matching.lot_size
        if intent.quantity % lot != 0:
            return (
                f"buy quantity {intent.quantity} is not a multiple of the "
                f"{lot}-share board lot"
            )
        if reference_price is None:
            return (
                "cannot estimate funds: no limit price and no market price "
                "seen for this symbol yet"
            )
        estimate = compute_fees(
            price=reference_price,
            quantity=intent.quantity,
            side=Side.BUY,
            schedule=fees,
        )
        cost = round(reference_price * intent.quantity + estimate.total, 2)
        if cost > account.cash + 0.005:
            return (
                f"insufficient funds: estimated cost {cost:.2f} exceeds "
                f"cash {account.cash:.2f}"
            )
    else:
        available = account.available_quantity(intent.symbol)
        if intent.quantity > available:
            return (
                f"sell quantity {intent.quantity} exceeds T+1 available "
                f"{available} of {intent.symbol}"
            )
        lot = matching.lot_size
        if available % lot != 0 and intent.quantity != available:
            return (
                f"odd-lot position of {available} shares must be sold in "
                "one shot (零股一次性卖出)"
            )

    if intent.price_mode is PriceMode.LIMIT:
        if prev_close is not None:
            limit_up, limit_down = limit_prices(
                prev_close,
                board=instrument.board,
                is_st=instrument.is_st,
                rules=matching,
            )
            if not (limit_down - 0.005 <= intent.limit_price <= limit_up + 0.005):
                return (
                    f"limit price {intent.limit_price:.2f} outside the day's "
                    f"price-limit band [{limit_down:.2f}, {limit_up:.2f}]"
                )
    return None
