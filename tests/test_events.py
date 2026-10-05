"""ExecutionEvent construction and payload-shape validation tests."""

from __future__ import annotations

import pytest
from pulsar_contracts import (
    ExecutionEvent,
    ExecutionEventType,
    OrderId,
)
from pydantic import ValidationError

from pulsar_exec import accepted, cancelled, error, fill, partial_fill, rejected
from conftest import make_fill, make_ts

ORDER_ID = OrderId("ord-test-1")


class TestFactoryHappyPaths:
    def test_accepted(self):
        event = accepted(ORDER_ID, make_ts())
        assert event.event_type is ExecutionEventType.ACCEPTED
        assert event.order_id == ORDER_ID
        assert event.fill is None
        assert event.reason is None

    def test_rejected(self):
        event = rejected(ORDER_ID, make_ts(), "insufficient buying power")
        assert event.event_type is ExecutionEventType.REJECTED
        assert event.reason == "insufficient buying power"

    def test_partial_fill(self):
        payload = make_fill(ORDER_ID, 10.0, 100)
        event = partial_fill(ORDER_ID, make_ts(), payload)
        assert event.event_type is ExecutionEventType.PARTIAL_FILL
        assert event.fill is payload

    def test_fill(self):
        payload = make_fill(ORDER_ID, 10.0, 300)
        event = fill(ORDER_ID, make_ts(), payload)
        assert event.event_type is ExecutionEventType.FILL
        assert event.fill is payload

    def test_cancelled_without_reason(self):
        event = cancelled(ORDER_ID, make_ts())
        assert event.event_type is ExecutionEventType.CANCELLED
        assert event.reason is None
        assert event.fill is None

    def test_cancelled_with_reason(self):
        event = cancelled(ORDER_ID, make_ts(), "user requested")
        assert event.reason == "user requested"

    def test_error(self):
        event = error(ORDER_ID, make_ts(), "gateway disconnected")
        assert event.event_type is ExecutionEventType.ERROR
        assert event.reason == "gateway disconnected"

    def test_events_are_immutable(self):
        event = accepted(ORDER_ID, make_ts())
        with pytest.raises(ValidationError):
            event.reason = "mutate"


class TestFactoryPreconditions:
    def test_rejected_requires_reason(self):
        with pytest.raises(ValueError):
            rejected(ORDER_ID, make_ts(), "")

    def test_error_requires_reason(self):
        with pytest.raises(ValueError):
            error(ORDER_ID, make_ts(), "")

    def test_factories_normalize_naive_timestamps(self):
        event = accepted(ORDER_ID, make_ts())  # naive Shanghai wall time
        assert event.ts.tzinfo is not None
        assert event.ts.utcoffset().total_seconds() == 8 * 3600


class TestContractShapeValidation:
    """The contract payload rules hold for every event we construct."""

    def test_fill_events_require_payload(self):
        with pytest.raises(ValidationError):
            ExecutionEvent(
                event_type=ExecutionEventType.FILL,
                order_id=ORDER_ID,
                ts=make_ts(),
            )
        with pytest.raises(ValidationError):
            ExecutionEvent(
                event_type=ExecutionEventType.PARTIAL_FILL,
                order_id=ORDER_ID,
                ts=make_ts(),
            )

    def test_fill_forbidden_on_non_fill_events(self):
        payload = make_fill(ORDER_ID, 10.0, 100)
        with pytest.raises(ValidationError):
            accepted_with_fill = ExecutionEvent(
                event_type=ExecutionEventType.ACCEPTED,
                order_id=ORDER_ID,
                ts=make_ts(),
                fill=payload,
            )

    def test_fill_order_id_must_match_event(self):
        foreign = make_fill(OrderId("ord-other"), 10.0, 100)
        with pytest.raises(ValidationError):
            ExecutionEvent(
                event_type=ExecutionEventType.FILL,
                order_id=ORDER_ID,
                ts=make_ts(),
                fill=foreign,
            )

    def test_rejected_and_error_require_reason(self):
        with pytest.raises(ValidationError):
            ExecutionEvent(
                event_type=ExecutionEventType.REJECTED,
                order_id=ORDER_ID,
                ts=make_ts(),
            )
        with pytest.raises(ValidationError):
            ExecutionEvent(
                event_type=ExecutionEventType.ERROR,
                order_id=ORDER_ID,
                ts=make_ts(),
            )

    def test_factory_outputs_always_satisfy_contract(self):
        for event in (
            accepted(ORDER_ID, make_ts()),
            rejected(ORDER_ID, make_ts(), "no"),
            partial_fill(ORDER_ID, make_ts(), make_fill(ORDER_ID, 10.0, 1)),
            fill(ORDER_ID, make_ts(), make_fill(ORDER_ID, 10.0, 1)),
            cancelled(ORDER_ID, make_ts()),
            error(ORDER_ID, make_ts(), "oops"),
        ):
            assert isinstance(event, ExecutionEvent)
