"""Run configuration for the backtest venue: fees, slippage, matching rules.

Per the Pulsar execution design, every rate and knob of the training
channel is configuration, never hard-coded policy:

* **Fees** — commission (both directions, with a per-trade minimum), stamp
  duty (sells only) and transfer fee; defaults follow current mainland
  practice and are updated by configuration when policy changes.
* **Slippage** — a fixed basis-point penalty plus an optional volume-impact
  term; defaults are conservative.
* **Matching** — bar-level fillability: the fraction of a bar's volume an
  order may consume, strict-vs-default price-boundary semantics, per-board
  price-limit rates and the board lot size.

All models are immutable; a venue run receives one frozen configuration.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Final, Mapping

from pulsar_contracts import Board

__all__ = [
    "CENT",
    "FeeSchedule",
    "SlippageModel",
    "MatchingRules",
    "DEFAULT_BOARD_LIMIT_RATES",
]

#: Money quantum of A-share prices and fees: one cent (CNY 0.01).
CENT: Final[Decimal] = Decimal("0.01")


@dataclass(frozen=True)
class FeeSchedule:
    """Per-fill fee rates, all configuration with policy defaults.

    Defaults (reviewed against current mainland practice; override via
    configuration when policy changes):

    ========================  ==================  ============================
    Fee                       Default             Direction
    ========================  ==================  ============================
    ``commission_rate``       0.0003  (0.03%)     buy and sell
    ``min_commission``        5.00 CNY per trade  buy and sell
    ``stamp_duty_rate``       0.0005  (0.05%)     sell only
    ``transfer_fee_rate``     0.00001 (0.001%)    buy and sell
    ========================  ==================  ============================
    """

    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_duty_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001

    def __post_init__(self) -> None:
        for name in (
            "commission_rate",
            "stamp_duty_rate",
            "transfer_fee_rate",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.min_commission < 0:
            raise ValueError("min_commission must be non-negative")


@dataclass(frozen=True)
class SlippageModel:
    """Slippage penalty applied to matched prices; conservative by default.

    The effective fill price of a buy is shifted up (a sell down) by::

        reference_price * (fixed_bps/1e4 + volume_impact_coeff * share)

    where ``share`` is the fraction of the bar's volume consumed by this
    fill (``0 <= share <= 1``). ``volume_impact_coeff`` defaults to 0 (the
    impact term is optional per the design); ``fixed_bps`` defaults to a
    deliberately conservative 5 bps.
    """

    fixed_bps: float = 5.0
    volume_impact_coeff: float = 0.0

    def __post_init__(self) -> None:
        if self.fixed_bps < 0:
            raise ValueError("fixed_bps must be non-negative")
        if self.volume_impact_coeff < 0:
            raise ValueError("volume_impact_coeff must be non-negative")


#: Default per-board daily price-limit rates of the A-share market:
#: main board ±10%, GEM/STAR ±20%, BSE ±30%. ST overrides the main-board
#: rate with ±5% (see :class:`MatchingRules.st_limit_rate`); GEM/STAR keep
#: their board rate regardless of the ST flag (post-2020 registration
#: reform these boards do not narrow the band for ST labels).
DEFAULT_BOARD_LIMIT_RATES: Final[Mapping[Board, float]] = {
    Board.MAIN: 0.10,
    Board.GEM: 0.20,
    Board.STAR: 0.20,
    Board.BSE: 0.30,
}


@dataclass(frozen=True)
class MatchingRules:
    """Bar-level matching knobs of the backtest venue.

    * ``max_volume_fraction`` — an order may consume at most this fraction
      of one bar's volume (prevents eating the whole bar); default 10%.
    * ``strict_price_boundary`` — strict mode: a limit order fills only
      when the bar *penetrates* the limit (``low < limit`` for buys,
      ``high > limit`` for sells). Default mode fills when the bar merely
      *touches or crosses* the limit (``low <= limit`` / ``high >= limit``).
    * ``price_limit_rates`` / ``st_limit_rate`` — per-board daily price
      limit bands used for out-of-band order rejection and one-line-board
      (一字板) detection.
    * ``lot_size`` — board lot of buy orders (A-share: 100 shares).
    """

    max_volume_fraction: float = 0.1
    strict_price_boundary: bool = False
    price_limit_rates: Mapping[Board, float] = field(
        default_factory=lambda: dict(DEFAULT_BOARD_LIMIT_RATES)
    )
    st_limit_rate: float = 0.05
    lot_size: int = 100

    def __post_init__(self) -> None:
        if not 0 < self.max_volume_fraction <= 1:
            raise ValueError("max_volume_fraction must be in (0, 1]")
        if self.st_limit_rate <= 0:
            raise ValueError("st_limit_rate must be positive")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be positive")
        for board, rate in self.price_limit_rates.items():
            if rate <= 0:
                raise ValueError(f"price-limit rate for {board.value} must be positive")

    def limit_rate(self, board: Board, is_st: bool) -> float:
        """Rate applicable to an instrument: ST narrows the main board only."""
        if is_st and board is Board.MAIN:
            return self.st_limit_rate
        return self.price_limit_rates.get(board, DEFAULT_BOARD_LIMIT_RATES[board])

    def with_strict_boundary(self, strict: bool) -> "MatchingRules":
        """Copy of the rules with the strict-boundary flag set to ``strict``."""
        return replace(self, strict_price_boundary=strict)
