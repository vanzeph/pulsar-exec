"""The live-trading gate: unlock environment, per-order and daily caps.

The Pulsar architecture baseline makes Live mode *locked by default* and
constrains it with two hard caps (per the execution design):

* **unlock** — an environment variable (name from run configuration, by
  default ``PULSAR_LIVE_CONFIRM``) must be explicitly set before any order
  may reach a broker; an optional expected token pins the value;
* **per-order cap** — ``notional = reference price x quantity`` of a single
  intent must not exceed ``max_order_value``;
* **daily cap** — the cumulative *traded* value of the trading day plus the
  new intent's notional must not exceed ``max_daily_traded_value`` (the
  pre-check is conservative: fills can only be booked under this budget).

Every rejection is *left on the record*: the gate keeps an append-only
:class:`RejectionRecord` trail (JSON-serialisable, one line per rejection)
so audits can prove which intent was refused, when, and against which cap.
Environment values are read through an injectable mapping — the code only
ever sees the *name* of the environment variable, never credentials.
"""

from __future__ import annotations

import enum
import json
import os
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path

from pulsar_contracts import OrderIntent

__all__ = [
    "GateRejectCode",
    "LiveGateConfig",
    "RejectionRecord",
    "GateDecision",
    "LiveGate",
]

#: Values that explicitly re-lock an otherwise present unlock variable.
_LOCKED_VALUES: frozenset[str] = frozenset({"", "0", "false", "no", "off"})


class GateRejectCode(enum.StrEnum):
    """Why the gate refused an intent."""

    NOT_UNLOCKED = "not_unlocked"
    UNPRICED = "unpriced"
    OVER_ORDER_LIMIT = "over_order_limit"
    OVER_DAILY_LIMIT = "over_daily_limit"
    GATE_HALTED = "gate_halted"


@dataclass(frozen=True)
class LiveGateConfig:
    """Configuration of the live gate; mirrors the run TOML ``[exec.miqmt]``.

    ``unlock_env`` is the *name* of the environment variable that unlocks
    live trading (the architecture example uses ``PULSAR_LIVE_CONFIRM``);
    ``unlock_token``, when set, additionally requires that exact value.
    """

    unlock_env: str = "PULSAR_LIVE_CONFIRM"
    unlock_token: str | None = None
    max_order_value: float = 50_000.0
    max_daily_traded_value: float = 500_000.0

    def __post_init__(self) -> None:
        if not self.unlock_env:
            raise ValueError("unlock_env must be a non-empty environment variable name")
        if self.unlock_token is not None and not self.unlock_token:
            raise ValueError("unlock_token must be non-empty when provided")
        if self.max_order_value <= 0:
            raise ValueError("max_order_value must be positive")
        if self.max_daily_traded_value <= 0:
            raise ValueError("max_daily_traded_value must be positive")


@dataclass(frozen=True)
class RejectionRecord:
    """One refused intent, kept for audit (拒单留痕)."""

    ts: datetime
    code: GateRejectCode
    reason: str
    run_id: str
    seq: int
    symbol: str
    side: str
    quantity: int
    notional: float | None
    max_order_value: float
    max_daily_traded_value: float
    day_traded_value: float

    def to_json_line(self) -> str:
        """Serialise to one JSON line (ISO timestamps, enum values)."""
        payload = asdict(self)
        payload["ts"] = self.ts.isoformat()
        payload["code"] = self.code.value
        payload["side"] = self.side
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class GateDecision:
    """Outcome of one gate check."""

    allowed: bool
    code: GateRejectCode | None = None
    reason: str | None = None

    @classmethod
    def _pass(cls) -> "GateDecision":
        return cls(allowed=True)

    @classmethod
    def _refuse(cls, code: GateRejectCode, reason: str) -> "GateDecision":
        return cls(allowed=False, code=code, reason=reason)


def _default_clock() -> datetime:
    return datetime.now(tz=None)


class LiveGate:
    """Stateful pre-trade gate in front of the live gateway.

    Thread-safe; owns the daily traded-value ledger (reset on day rollover
    of the injected clock) and the halt flag used by safe shutdown and the
    read-only alert state.
    """

    def __init__(
        self,
        config: LiveGateConfig | None = None,
        *,
        env: Mapping[str, str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config if config is not None else LiveGateConfig()
        self._env: Mapping[str, str] | None = env  # None -> read os.environ lazily
        self._clock = clock or _default_clock
        self._lock = threading.RLock()

        self._day: date = self._clock().date()
        self._day_traded_value = 0.0
        self._daily_cap_breached = False
        self._halt_reason: str | None = None
        self._rejections: list[RejectionRecord] = []

    # -- configuration ----------------------------------------------------
    @property
    def config(self) -> LiveGateConfig:
        """The frozen gate configuration."""
        return self._config

    # -- unlock -------------------------------------------------------------
    def is_unlocked(self) -> bool:
        """Whether the unlock environment variable explicitly unlocks live."""
        value = self._read_env(self._config.unlock_env)
        if value is None or value.strip().lower() in _LOCKED_VALUES:
            return False
        if self._config.unlock_token is not None:
            return value.strip() == self._config.unlock_token
        return True

    def _read_env(self, name: str) -> str | None:
        source = os.environ if self._env is None else self._env
        return source.get(name)

    # -- daily traded-value ledger -------------------------------------------
    @property
    def day_traded_value(self) -> float:
        """Cumulative traded value booked today (fills, both directions)."""
        with self._lock:
            self._roll_day_if_needed()
            return round(self._day_traded_value, 2)

    @property
    def daily_cap_breached(self) -> bool:
        """Whether a booked fill already pushed today past the daily cap."""
        with self._lock:
            self._roll_day_if_needed()
            return self._daily_cap_breached

    @property
    def remaining_daily_headroom(self) -> float:
        """Daily cap minus today's traded value (never negative)."""
        with self._lock:
            self._roll_day_if_needed()
            return round(
                max(
                    0.0,
                    self._config.max_daily_traded_value - self._day_traded_value,
                ),
                2,
            )

    def record_fill(self, value: float) -> None:
        """Book one confirmed fill's traded value into today's ledger."""
        if value < 0:
            raise ValueError("fill value must be non-negative")
        with self._lock:
            self._roll_day_if_needed()
            self._day_traded_value += value
            if self._day_traded_value > self._config.max_daily_traded_value + 1e-9:
                # A booked fill cannot be un-booked: the gate stays closed
                # for the rest of the day (conservative, auditable).
                self._daily_cap_breached = True

    def _roll_day_if_needed(self) -> None:
        today = self._clock().date()
        if today != self._day:
            self._day = today
            self._day_traded_value = 0.0
            self._daily_cap_breached = False

    # -- halt ----------------------------------------------------------------
    @property
    def halted(self) -> bool:
        """Whether the gate is administratively closed (shutdown/alert)."""
        with self._lock:
            return self._halt_reason is not None

    @property
    def halt_reason(self) -> str | None:
        with self._lock:
            return self._halt_reason

    def halt(self, reason: str) -> None:
        """Close the gate: every further intent is refused with a trail."""
        if not reason:
            raise ValueError("halt reason must be non-empty")
        with self._lock:
            self._halt_reason = reason

    def resume(self) -> None:
        """Re-open the gate (operator action after alert acknowledgement)."""
        with self._lock:
            self._halt_reason = None

    # -- the check -------------------------------------------------------------
    def check(
        self,
        intent: OrderIntent,
        *,
        reference_price: float | None,
    ) -> GateDecision:
        """Decide whether ``intent`` may go to the broker.

        Rejections are recorded in the trail (one :class:`RejectionRecord`
        per refusal).  ``reference_price`` is the price used to size the
        intent's notional (limit price or a subscribed market price); a
        missing reference is itself a rejection — an unsizable intent is a
        risk, not a guess.
        """
        with self._lock:
            self._roll_day_if_needed()

            reason: str | None = None
            code: GateRejectCode | None = None
            notional: float | None = (
                round(reference_price * intent.quantity, 2)
                if reference_price is not None
                else None
            )

            if self._halt_reason is not None:
                code = GateRejectCode.GATE_HALTED
                reason = f"live gate halted: {self._halt_reason}"
            elif not self.is_unlocked():
                code = GateRejectCode.NOT_UNLOCKED
                reason = (
                    f"live trading locked: environment variable "
                    f"{self._config.unlock_env} is not explicitly set"
                )
            elif reference_price is None:
                code = GateRejectCode.UNPRICED
                reason = (
                    "cannot size intent: no limit price and no market reference "
                    "price available for the gate check"
                )
            else:
                assert notional is not None  # reference_price is not None here
                if notional > self._config.max_order_value + 1e-9:
                    code = GateRejectCode.OVER_ORDER_LIMIT
                    reason = (
                        f"order notional {notional:.2f} CNY exceeds per-order "
                        f"cap {self._config.max_order_value:.2f} CNY"
                    )
                elif (
                    self._daily_cap_breached
                    or self._day_traded_value + notional
                    > self._config.max_daily_traded_value + 1e-9
                ):
                    code = GateRejectCode.OVER_DAILY_LIMIT
                    reason = (
                        f"order notional {notional:.2f} CNY exceeds the daily "
                        f"budget: {self._day_traded_value:.2f} CNY already "
                        f"traded today against a "
                        f"{self._config.max_daily_traded_value:.2f} CNY cap"
                    )

            if code is not None and reason is not None:
                record = RejectionRecord(
                    ts=self._clock(),
                    code=code,
                    reason=reason,
                    run_id=intent.idempotency_key.run_id,
                    seq=intent.idempotency_key.seq,
                    symbol=intent.symbol,
                    side=intent.side.value,
                    quantity=intent.quantity,
                    notional=notional,
                    max_order_value=self._config.max_order_value,
                    max_daily_traded_value=self._config.max_daily_traded_value,
                    day_traded_value=round(self._day_traded_value, 2),
                )
                self._rejections.append(record)
                return GateDecision._refuse(code, reason)
            return GateDecision._pass()

    # -- audit trail -----------------------------------------------------------
    @property
    def rejections(self) -> tuple[RejectionRecord, ...]:
        """The full rejection trail, oldest first."""
        with self._lock:
            return tuple(self._rejections)

    def rejection_log_lines(self) -> str:
        """The trail as JSON lines (one object per refusal)."""
        return "".join(record.to_json_line() + "\n" for record in self.rejections)

    def write_rejection_log(self, path: Path | str) -> Path:
        """Persist the trail to ``path`` (JSON lines); returns the path."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.rejection_log_lines(), encoding="utf-8")
        return target
