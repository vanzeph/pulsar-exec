"""Order state machine tests: every legal transition, every illegal one.

Covers the full 6x6 transition matrix (7 legal accepted, 29 illegal
rejected), the event tables, and event-driven advancement of immutable
order snapshots across complete lifecycles.
"""

from __future__ import annotations

import pytest
from pulsar_contracts import (
    SHANGHAI_TZ,
    ExecutionEventType,
    Order,
    OrderId,
    OrderStatus,
    Side,
)

from pulsar_exec import (
    EVENT_ALLOWED_CURRENT,
    EVENT_TARGET_STATUS,
    LEGAL_TRANSITIONS,
    ORDER_STATE_MACHINE,
    EventStateMismatchError,
    ExecutionStateError,
    FillConsistencyError,
    IllegalOrderTransitionError,
    accepted,
    advance_order,
    cancelled,
    error,
    fill,
    partial_fill,
    rejected,
    validate_event_for_order,
)
from conftest import make_fill, make_intent, make_order, make_ts

ALL_STATUSES = sorted(OrderStatus, key=lambda s: s.value)

TERMINAL_STATUSES = [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]
LIVE_STATUSES = [
    OrderStatus.CREATED,
    OrderStatus.SUBMITTED,
    OrderStatus.PARTIALLY_FILLED,
]

ALL_PAIRS = [(current, target) for current in ALL_STATUSES for target in ALL_STATUSES]
ILLEGAL_PAIRS = [pair for pair in ALL_PAIRS if pair not in LEGAL_TRANSITIONS]


class TestLegalTransitions:
    @pytest.mark.parametrize(("current", "target"), sorted(LEGAL_TRANSITIONS))
    def test_every_legal_transition_passes(self, current: OrderStatus, target: OrderStatus):
        assert ORDER_STATE_MACHINE.is_legal(current, target)
        assert ORDER_STATE_MACHINE.require_legal(current, target) is target

    def test_whitelist_matches_documented_design(self):
        """Exactly the seven transitions of the Pulsar execution design."""
        assert LEGAL_TRANSITIONS == frozenset(
            {
                (OrderStatus.CREATED, OrderStatus.SUBMITTED),
                (OrderStatus.CREATED, OrderStatus.REJECTED),
                (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED),
                (OrderStatus.SUBMITTED, OrderStatus.FILLED),
                (OrderStatus.SUBMITTED, OrderStatus.CANCELLED),
                (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED),
                (OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED),
            }
        )
        assert len(LEGAL_TRANSITIONS) == 7

    def test_each_legal_transition_has_documented_reason(self):
        reasons = {
            (OrderStatus.CREATED, OrderStatus.SUBMITTED): "gateway accepted",
            (OrderStatus.CREATED, OrderStatus.REJECTED): "validation failed",
            (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED): "partial execution",
            (OrderStatus.SUBMITTED, OrderStatus.FILLED): "filled in one shot",
            (OrderStatus.SUBMITTED, OrderStatus.CANCELLED): "cancel succeeded",
            (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED): "remainder filled",
            (OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED): "cancelled, remainder voided",
        }
        assert set(reasons) == set(LEGAL_TRANSITIONS)


class TestIllegalTransitions:
    @pytest.mark.parametrize(("current", "target"), ILLEGAL_PAIRS)
    def test_every_illegal_transition_rejected(
        self, current: OrderStatus, target: OrderStatus
    ):
        assert not ORDER_STATE_MACHINE.is_legal(current, target)
        with pytest.raises(IllegalOrderTransitionError) as excinfo:
            ORDER_STATE_MACHINE.require_legal(current, target)
        assert excinfo.value.current is current
        assert excinfo.value.target is target
        assert current.value in str(excinfo.value)
        assert target.value in str(excinfo.value)

    def test_illegal_matrix_is_exhaustive(self):
        """36 possible pairs = 7 legal + 29 illegal, nothing unaccounted."""
        assert len(ALL_PAIRS) == 36
        assert len(ILLEGAL_PAIRS) == 29
        assert len(set(LEGAL_TRANSITIONS) | set(ILLEGAL_PAIRS)) == 36

    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    @pytest.mark.parametrize("target", ALL_STATUSES)
    def test_terminal_states_have_no_outgoing(
        self, status: OrderStatus, target: OrderStatus
    ):
        assert (status, target) not in LEGAL_TRANSITIONS

    @pytest.mark.parametrize("status", LIVE_STATUSES)
    def test_live_states_have_outgoing(self, status: OrderStatus):
        assert any(current == status for current, _ in LEGAL_TRANSITIONS)

    @pytest.mark.parametrize("status", ALL_STATUSES)
    def test_self_transition_is_illegal(self, status: OrderStatus):
        assert (status, status) not in LEGAL_TRANSITIONS

    @pytest.mark.parametrize("status", ALL_STATUSES)
    def test_backwards_and_cross_transitions_illegal(self, status: OrderStatus):
        """No skipping (Created->Filled), no revival (Filled->Created...)."""
        assert (OrderStatus.CREATED, OrderStatus.FILLED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.CREATED, OrderStatus.PARTIALLY_FILLED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.CREATED, OrderStatus.CANCELLED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.SUBMITTED, OrderStatus.CREATED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.PARTIALLY_FILLED, OrderStatus.SUBMITTED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.REJECTED, OrderStatus.SUBMITTED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.FILLED, OrderStatus.CANCELLED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.CANCELLED, OrderStatus.FILLED) not in LEGAL_TRANSITIONS
        assert (OrderStatus.PARTIALLY_FILLED, OrderStatus.REJECTED) not in LEGAL_TRANSITIONS
        assert (status, OrderStatus.CREATED) not in LEGAL_TRANSITIONS


class TestTerminality:
    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    def test_terminal_detection(self, status: OrderStatus):
        assert ORDER_STATE_MACHINE.is_terminal(status)
        assert status.is_terminal  # agrees with the contract enum

    @pytest.mark.parametrize("status", LIVE_STATUSES)
    def test_live_detection(self, status: OrderStatus):
        assert not ORDER_STATE_MACHINE.is_terminal(status)
        assert not status.is_terminal


class TestEventTables:
    def test_targets_match_design(self):
        assert EVENT_TARGET_STATUS == {
            ExecutionEventType.ACCEPTED: OrderStatus.SUBMITTED,
            ExecutionEventType.REJECTED: OrderStatus.REJECTED,
            ExecutionEventType.PARTIAL_FILL: OrderStatus.PARTIALLY_FILLED,
            ExecutionEventType.FILL: OrderStatus.FILLED,
            ExecutionEventType.CANCELLED: OrderStatus.CANCELLED,
            ExecutionEventType.ERROR: None,
        }

    def test_every_event_pair_is_a_legal_transition(self):
        """Event tables can never imply a transition off the whitelist.

        Pairs where target equals current are "stay" semantics (a later
        PARTIAL_FILL on a PARTIALLY_FILLED order) and are excluded, exactly
        like in the module-level consistency guard.
        """
        for event_type, allowed in EVENT_ALLOWED_CURRENT.items():
            target = EVENT_TARGET_STATUS[event_type]
            if target is None:
                continue
            for current in allowed:
                if current is target:
                    continue
                assert (current, target) in LEGAL_TRANSITIONS

    def test_error_only_event_without_transition(self):
        assert [e for e, t in EVENT_TARGET_STATUS.items() if t is None] == [
            ExecutionEventType.ERROR
        ]

    @pytest.mark.parametrize("event_type", list(ExecutionEventType))
    def test_validate_event_returns_expected_status(self, event_type):
        for current in EVENT_ALLOWED_CURRENT[event_type]:
            expected = EVENT_TARGET_STATUS[event_type] or current
            assert ORDER_STATE_MACHINE.validate_event(current, event_type) is expected

    @pytest.mark.parametrize("event_type", list(ExecutionEventType))
    def test_validate_event_rejects_disallowed_current(self, event_type):
        allowed = EVENT_ALLOWED_CURRENT[event_type]
        for current in ALL_STATUSES:
            if current in allowed:
                continue
            with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
                ORDER_STATE_MACHINE.validate_event(current, event_type)

    @pytest.mark.parametrize("event_type", list(ExecutionEventType))
    @pytest.mark.parametrize("status", TERMINAL_STATUSES)
    def test_no_event_applies_to_terminal_orders(
        self, event_type: ExecutionEventType, status: OrderStatus
    ):
        with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
            ORDER_STATE_MACHINE.validate_event(status, event_type)


class TestValidateEventForOrder:
    def test_event_for_other_order_rejected(self):
        order = make_order()
        event = accepted(OrderId("ord-other"), make_ts())
        with pytest.raises(ExecutionStateError):
            validate_event_for_order(event, order)

    def test_fill_symbol_mismatch(self):
        order = make_order(status=OrderStatus.SUBMITTED)
        bad = make_fill(order.order_id, 10.0, 100, symbol="000001")
        event = partial_fill(order.order_id, make_ts(), bad)
        with pytest.raises(FillConsistencyError):
            validate_event_for_order(event, order)

    def test_fill_side_mismatch(self):
        order = make_order(status=OrderStatus.SUBMITTED)
        bad = make_fill(order.order_id, 10.0, 100, side=Side.SELL)
        event = partial_fill(order.order_id, make_ts(), bad)
        with pytest.raises(FillConsistencyError):
            validate_event_for_order(event, order)

    def test_partial_fill_completing_order_must_be_fill(self):
        order = make_order(status=OrderStatus.SUBMITTED, intent=make_intent(quantity=100))
        completing = make_fill(order.order_id, 10.0, 100)
        event = partial_fill(order.order_id, make_ts(), completing)
        with pytest.raises(FillConsistencyError):
            validate_event_for_order(event, order)

    def test_fill_not_completing_order_rejected(self):
        order = make_order(status=OrderStatus.SUBMITTED, intent=make_intent(quantity=300))
        short = make_fill(order.order_id, 10.0, 200)
        event = fill(order.order_id, make_ts(), short)
        with pytest.raises(FillConsistencyError):
            validate_event_for_order(event, order)

    def test_overfill_rejected(self):
        order = make_order(
            status=OrderStatus.PARTIALLY_FILLED,
            intent=make_intent(quantity=300),
            filled_quantity=250,
            avg_fill_price=10.0,
        )
        too_much = make_fill(order.order_id, 10.0, 100)
        event = partial_fill(order.order_id, make_ts(), too_much)
        with pytest.raises(FillConsistencyError):
            validate_event_for_order(event, order)

    def test_error_keeps_status_but_requires_live_order(self):
        live = make_order(status=OrderStatus.SUBMITTED)
        assert (
            validate_event_for_order(error(live.order_id, make_ts(), "link down"), live)
            is OrderStatus.SUBMITTED
        )
        filled = make_order(
            status=OrderStatus.FILLED,
            filled_quantity=300,
            avg_fill_price=10.0,
        )
        with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
            validate_event_for_order(error(filled.order_id, make_ts(), "late error"), filled)


class TestAdvanceOrder:
    def test_full_lifecycle_created_submitted_partial_filled(self):
        order = make_order()
        assert order.status is OrderStatus.CREATED

        step1 = advance_order(order, accepted(order.order_id, make_ts(1)))
        assert step1.status is OrderStatus.SUBMITTED
        assert step1.updated_at == make_ts(1).replace(tzinfo=SHANGHAI_TZ)

        first = make_fill(order.order_id, 10.0, 100, seq=1)
        step2 = advance_order(step1, partial_fill(order.order_id, make_ts(2), first))
        assert step2.status is OrderStatus.PARTIALLY_FILLED
        assert step2.filled_quantity == 100
        assert step2.avg_fill_price == pytest.approx(10.0)

        second = make_fill(order.order_id, 13.0, 200, seq=2)
        step3 = advance_order(step2, fill(order.order_id, make_ts(3), second))
        assert step3.status is OrderStatus.FILLED
        assert step3.filled_quantity == 300
        # volume-weighted: (100*10 + 200*13) / 300
        assert step3.avg_fill_price == pytest.approx(12.0)

        # terminal: nothing further applies
        with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
            advance_order(step3, cancelled(order.order_id, make_ts(4)))

    def test_lifecycle_created_submitted_filled(self):
        order = make_order()
        step1 = advance_order(order, accepted(order.order_id, make_ts()))
        whole = make_fill(order.order_id, 10.0, 300)
        step2 = advance_order(step1, fill(order.order_id, make_ts(), whole))
        assert step2.status is OrderStatus.FILLED
        assert step2.filled_quantity == 300

    def test_lifecycle_created_submitted_cancelled(self):
        order = make_order()
        step1 = advance_order(order, accepted(order.order_id, make_ts()))
        step2 = advance_order(step1, cancelled(order.order_id, make_ts()))
        assert step2.status is OrderStatus.CANCELLED
        assert step2.filled_quantity == 0
        assert step2.reject_reason is None

    def test_lifecycle_created_rejected(self):
        order = make_order()
        step = advance_order(order, rejected(order.order_id, make_ts(), "limit price invalid"))
        assert step.status is OrderStatus.REJECTED
        assert step.reject_reason == "limit price invalid"
        assert step.filled_quantity == 0

    def test_lifecycle_partial_then_cancelled_keeps_fills(self):
        order = make_order()
        step1 = advance_order(order, accepted(order.order_id, make_ts()))
        step2 = advance_order(
            step1, partial_fill(order.order_id, make_ts(), make_fill(order.order_id, 10.0, 100))
        )
        step3 = advance_order(step2, cancelled(order.order_id, make_ts(2), "user request"))
        assert step3.status is OrderStatus.CANCELLED
        assert step3.filled_quantity == 100
        assert step3.avg_fill_price == pytest.approx(10.0)

    def test_error_event_leaves_state_unchanged(self):
        order = make_order(status=OrderStatus.SUBMITTED)
        after = advance_order(order, error(order.order_id, make_ts(), "disconnect"))
        assert after.status is OrderStatus.SUBMITTED
        assert after.filled_quantity == 0

    def test_advance_is_immutable(self):
        order = make_order()
        snapshot = order.model_dump()
        advance_order(order, accepted(order.order_id, make_ts()))
        assert order.model_dump() == snapshot

    def test_advance_revalidates_invariants(self):
        """The returned snapshot goes through Order validation again."""
        order = make_order(status=OrderStatus.SUBMITTED)
        step = advance_order(
            order, partial_fill(order.order_id, make_ts(), make_fill(order.order_id, 10.0, 100))
        )
        assert isinstance(step, Order)
        assert step.filled_quantity > 0 and step.avg_fill_price is not None

    def test_advance_rejects_illegal_sequence(self):
        order = make_order()
        with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
            advance_order(order, fill(order.order_id, make_ts(), make_fill(order.order_id, 10.0, 300)))
        with pytest.raises((EventStateMismatchError, IllegalOrderTransitionError)):
            advance_order(order, partial_fill(order.order_id, make_ts(), make_fill(order.order_id, 10.0, 100)))
