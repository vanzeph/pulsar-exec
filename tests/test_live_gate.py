"""LiveGate tests: unlock, caps, halt, rollover and the rejection trail.

The three mandated rejection classes (未解锁 / 超单笔 / 超单日) each refuse
the intent AND leave an audit record.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from fake_broker import MutableClock
from conftest import make_intent, make_ts

from pulsar_exec.live.gate import (
    GateRejectCode,
    LiveGate,
    LiveGateConfig,
)

UNLOCK_ENV = "PULSAR_TEST_LIVE_UNLOCK"


def make_gate(
    *,
    env: dict[str, str] | None = None,
    config: LiveGateConfig | None = None,
    clock: MutableClock | None = None,
) -> tuple[LiveGate, MutableClock]:
    clock = clock or MutableClock(make_ts())
    gate = LiveGate(
        config
        or LiveGateConfig(
            unlock_env=UNLOCK_ENV,
            max_order_value=10_000_000.0,
            max_daily_traded_value=10_000_000.0,
        ),
        env=env if env is not None else {},
        clock=clock,
    )
    return gate, clock


def unlocked_env() -> dict[str, str]:
    return {UNLOCK_ENV: "1"}


class TestUnlock:
    def test_locked_by_default_rejects_and_trails(self) -> None:
        gate, _ = make_gate(env={})
        decision = gate.check(make_intent(), reference_price=1800.0)
        assert not decision.allowed
        assert decision.code is GateRejectCode.NOT_UNLOCKED
        assert UNLOCK_ENV in decision.reason
        trail = gate.rejections
        assert len(trail) == 1
        assert trail[0].code is GateRejectCode.NOT_UNLOCKED
        assert trail[0].notional == pytest.approx(1800.0 * 300)

    def test_unlocked_env_allows(self) -> None:
        gate, _ = make_gate(env=unlocked_env())
        decision = gate.check(make_intent(), reference_price=1800.0)
        assert decision.allowed
        assert gate.rejections == ()

    def test_explicit_lock_values_stay_locked(self) -> None:
        for value in ("0", "false", "no", "off", ""):
            gate, _ = make_gate(env={UNLOCK_ENV: value})
            decision = gate.check(make_intent(), reference_price=1800.0)
            assert decision.code is GateRejectCode.NOT_UNLOCKED, value

    def test_unlock_token_must_match(self) -> None:
        config = LiveGateConfig(
            unlock_env=UNLOCK_ENV,
            unlock_token="CONFIRM-42",
            max_order_value=10_000_000.0,
            max_daily_traded_value=10_000_000.0,
        )
        wrong, _ = make_gate(env={UNLOCK_ENV: "1"}, config=config)
        assert wrong.check(make_intent(), reference_price=1800.0).code is (
            GateRejectCode.NOT_UNLOCKED
        )
        right, _ = make_gate(env={UNLOCK_ENV: "CONFIRM-42"}, config=config)
        assert right.check(make_intent(), reference_price=1800.0).allowed


class TestCaps:
    def test_over_per_order_cap_rejects_and_trails(self) -> None:
        config = LiveGateConfig(unlock_env=UNLOCK_ENV, max_order_value=100_000.0)
        gate, _ = make_gate(env=unlocked_env(), config=config)
        decision = gate.check(make_intent(quantity=100), reference_price=1800.0)
        assert not decision.allowed
        assert decision.code is GateRejectCode.OVER_ORDER_LIMIT
        assert decision.reason is not None and "180000" in decision.reason.replace(",", "")
        record = gate.rejections[0]
        assert record.code is GateRejectCode.OVER_ORDER_LIMIT
        assert record.notional == pytest.approx(180_000.0)

    def test_within_caps_passes(self) -> None:
        config = LiveGateConfig(unlock_env=UNLOCK_ENV, max_order_value=100_000.0)
        gate, _ = make_gate(env=unlocked_env(), config=config)
        assert gate.check(make_intent(quantity=10), reference_price=1800.0).allowed

    def test_over_daily_cumulative_cap_rejects(self) -> None:
        config = LiveGateConfig(
            unlock_env=UNLOCK_ENV,
            max_order_value=1_000_000.0,
            max_daily_traded_value=500_000.0,
        )
        gate, _ = make_gate(env=unlocked_env(), config=config)
        # fills worth 400k booked earlier today
        gate.record_fill(400_000.0)
        decision = gate.check(make_intent(quantity=100), reference_price=1800.0)
        assert not decision.allowed
        assert decision.code is GateRejectCode.OVER_DAILY_LIMIT
        record = gate.rejections[0]
        assert record.day_traded_value == pytest.approx(400_000.0)

    def test_daily_headroom_precheck(self) -> None:
        config = LiveGateConfig(
            unlock_env=UNLOCK_ENV, max_order_value=1_000_000.0, max_daily_traded_value=540_000.0
        )
        gate, _ = make_gate(env=unlocked_env(), config=config)
        gate.record_fill(400_000.0)
        # 400k + 180k > 540k -> refused even though the single order is fine
        decision = gate.check(make_intent(quantity=100), reference_price=1800.0)
        assert decision.code is GateRejectCode.OVER_DAILY_LIMIT

    def test_record_fill_breach_closes_rest_of_day(self) -> None:
        config = LiveGateConfig(unlock_env=UNLOCK_ENV, max_daily_traded_value=100_000.0)
        gate, _ = make_gate(env=unlocked_env(), config=config)
        gate.record_fill(150_000.0)  # over the cap already (cannot unbook)
        assert gate.daily_cap_breached
        assert gate.remaining_daily_headroom == 0.0
        decision = gate.check(make_intent(quantity=1), reference_price=10.0)
        assert decision.code is GateRejectCode.OVER_DAILY_LIMIT


class TestHaltAndRollover:
    def test_halted_gate_refuses_with_trail(self) -> None:
        gate, _ = make_gate(env=unlocked_env())
        gate.halt("safe shutdown in progress")
        decision = gate.check(make_intent(), reference_price=1800.0)
        assert decision.code is GateRejectCode.GATE_HALTED
        assert "safe shutdown" in (decision.reason or "")
        gate.resume()
        assert gate.check(make_intent(), reference_price=1800.0).allowed

    def test_day_rollover_resets_traded_value(self) -> None:
        config = LiveGateConfig(unlock_env=UNLOCK_ENV, max_daily_traded_value=100_000.0)
        gate, clock = make_gate(env=unlocked_env(), config=config)
        gate.record_fill(100_000.0)
        assert gate.check(make_intent(quantity=10), reference_price=10.0).code is (
            GateRejectCode.OVER_DAILY_LIMIT
        )
        clock.now = clock.now + timedelta(days=1)
        assert gate.day_traded_value == 0.0
        assert gate.check(make_intent(quantity=10), reference_price=10.0).allowed

    def test_unpriced_intent_rejected(self) -> None:
        gate, _ = make_gate(env=unlocked_env())
        decision = gate.check(make_intent(), reference_price=None)
        assert decision.code is GateRejectCode.UNPRICED


class TestTrail:
    def test_rejection_log_is_jsonl(self, tmp_path) -> None:
        gate, _ = make_gate(env=unlocked_env())
        gate.halt("cap audit")
        gate.check(make_intent(seq=1), reference_price=1800.0)  # halted
        gate.resume()
        gate.check(make_intent(seq=2), reference_price=None)  # unpriced
        lines = gate.rejection_log_lines().splitlines()
        assert len(lines) == 2
        payloads = [json.loads(line) for line in lines]
        assert payloads[0]["code"] == "gate_halted"
        assert payloads[1]["code"] == "unpriced"
        assert payloads[0]["run_id"] == "run-20261005-0001"
        assert payloads[0]["seq"] == 1

        target = tmp_path / "audit" / "rejections.jsonl"
        written = gate.write_rejection_log(target)
        assert written.exists()
        assert len(written.read_text().splitlines()) == 2

    def test_config_validation(self) -> None:
        with pytest.raises(ValueError):
            LiveGateConfig(max_order_value=0)
        with pytest.raises(ValueError):
            LiveGateConfig(max_daily_traded_value=-1)
        with pytest.raises(ValueError):
            LiveGateConfig(unlock_env="")
