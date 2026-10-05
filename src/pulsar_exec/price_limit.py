"""A-share daily price limits: band computation and one-line-board detection.

The bands follow the Pulsar execution design: main board ±10%, GEM/STAR
±20%, ST ±5% (narrowing the main board only), each rounded to the cent
with ROUND_HALF_UP — the exchange's rounding discipline (e.g. a previous
close of 9.87 caps a main-board stock at ``9.87 * 1.1 = 10.857 -> 10.86``
and floors it at ``9.87 * 0.9 = 8.883 -> 8.88``).

A **one-line board** (一字板) is a bar whose open, high, low and close all
sit exactly on the limit-up or limit-down price: the entire bar traded (if
at all) inside one locked price, so no resting order can execute against
it — buys at a sealed limit-up and sells at a sealed limit-down must not
fill.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from pulsar_contracts import Bar, Board

from .config import CENT, MatchingRules

__all__ = ["limit_prices", "limit_rate_for", "is_one_line_board"]


def limit_rate_for(board: Board, is_st: bool, rules: MatchingRules) -> float:
    """Applicable daily limit rate: ST narrows the main board to ±5%."""
    return rules.limit_rate(board, is_st)


def limit_prices(
    prev_close: float,
    *,
    board: Board,
    is_st: bool,
    rules: MatchingRules,
) -> tuple[float, float]:
    """Return ``(limit_up, limit_down)`` for one instrument and day.

    Both prices are rounded to the cent with ROUND_HALF_UP from
    ``prev_close * (1 ± rate)`` computed in exact decimal arithmetic.
    """
    if prev_close <= 0:
        raise ValueError("prev_close must be positive")

    rate = Decimal(str(limit_rate_for(board, is_st, rules)))
    base = Decimal(str(prev_close))
    up = (base * (Decimal(1) + rate)).quantize(CENT, rounding=ROUND_HALF_UP)
    down = (base * (Decimal(1) - rate)).quantize(CENT, rounding=ROUND_HALF_UP)
    return float(up), float(down)


def _same_cent(a: float, b: float) -> bool:
    """Compare two prices at cent precision (float-safe)."""
    return abs(a - b) < 0.005


def is_one_line_board(
    bar: Bar,
    prev_close: float | None,
    *,
    board: Board,
    is_st: bool,
    rules: MatchingRules,
) -> bool:
    """Return ``True`` iff ``bar`` is a sealed one-line limit board.

    A bar qualifies when open == high == low == close **and** either

    * that single price equals the computed limit-up or limit-down price
      for the instrument (the standard 一字板), or
    * ``prev_close`` is unknown — a fully locked bar whose band cannot be
      verified is treated as sealed as well (conservative default: no
      fills against a bar that never traded away from one price).
    """
    if not (bar.open == bar.high == bar.low == bar.close):
        return False
    if prev_close is None:
        return True
    limit_up, limit_down = limit_prices(
        prev_close, board=board, is_st=is_st, rules=rules
    )
    return _same_cent(bar.close, limit_up) or _same_cent(bar.close, limit_down)
