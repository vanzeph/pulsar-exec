"""The shared Pulsar order state machine: transition whitelist and validation.

Every execution channel (backtest venue, paper broker, live gateway) reports
order progress against the single state machine defined by the Pulsar
execution design::

    Created --> Submitted --> PartiallyFilled --> Filled
            --> Rejected                     --> Cancelled

Concretely, the legal transition whitelist is:

======================  ========================  ==============================
From                    To                        Meaning
======================  ========================  ==============================
``CREATED``             ``SUBMITTED``             gateway accepted the order
``CREATED``             ``REJECTED``              validation failed
``SUBMITTED``           ``PARTIALLY_FILLED``      first partial execution
``SUBMITTED``           ``FILLED``                filled in one shot
``SUBMITTED``           ``CANCELLED``             cancel succeeded
``PARTIALLY_FILLED``    ``FILLED``                remainder filled
``PARTIALLY_FILLED``    ``CANCELLED``             cancelled, remainder voided
======================  ========================  ==============================

``FILLED``, ``CANCELLED`` and ``REJECTED`` are terminal. Any transition
outside the whitelist is illegal and must be rejected; channel-private
intermediate states have to be digested inside the adapter before reporting.

Events map onto status transitions as follows (:class:`ExecutionEventType`
``ERROR`` deliberately maps to *no* transition: an unconfirmed order keeps
its current state and converges via ``query``/reconciliation):

======================  ==========================  ==============================
Event                   Allowed current statuses   Resulting status
======================  ==========================  ==============================
``ACCEPTED``            ``CREATED``                ``SUBMITTED``
``REJECTED``            ``CREATED``                ``REJECTED``
``PARTIAL_FILL``        ``SUBMITTED`` or ``PARTIALLY_FILLED``  ``PARTIALLY_FILLED``
``FILL``                ``SUBMITTED`` or ``PARTIALLY_FILLED``  ``FILLED``
``CANCELLED``           ``SUBMITTED`` or ``PARTIALLY_FILLED``  ``CANCELLED``
``ERROR``               any non-terminal           unchanged
======================  ==========================  ==============================
"""

from __future__ import annotations

from typing import Final, Mapping

from pulsar_contracts import (
    ExecutionEvent,
    ExecutionEventType,
    Order,
    OrderStatus,
)

__all__ = [
    "ExecutionStateError",
    "IllegalOrderTransitionError",
    "EventStateMismatchError",
    "FillConsistencyError",
    "LEGAL_TRANSITIONS",
    "EVENT_TARGET_STATUS",
    "EVENT_ALLOWED_CURRENT",
    "OrderStateMachine",
    "ORDER_STATE_MACHINE",
    "advance_order",
    "validate_event_for_order",
]


class ExecutionStateError(ValueError):
    """Base class for order-state violations raised by this package."""


class IllegalOrderTransitionError(ExecutionStateError):
    """A status transition outside the legal whitelist was attempted."""

    def __init__(
        self,
        current: OrderStatus,
        target: OrderStatus,
        event_type: ExecutionEventType | None = None,
    ) -> None:
        self.current = current
        self.target = target
        self.event_type = event_type
        source = f"{event_type.value} event" if event_type is not None else "explicit transition"
        super().__init__(
            f"Illegal order state transition {current.value} -> {target.value} "
            f"({source}); the Pulsar order state machine only allows the "
            f"documented whitelist of transitions"
        )


class EventStateMismatchError(IllegalOrderTransitionError):
    """An event type that cannot be applied to the order's current status."""


class FillConsistencyError(ExecutionStateError):
    """A fill event whose quantity is inconsistent with the order book."""


#: The complete legal transition whitelist of the shared order state machine.
LEGAL_TRANSITIONS: Final[frozenset[tuple[OrderStatus, OrderStatus]]] = frozenset(
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

#: Status each event type drives the order into; ``None`` means "no transition".
EVENT_TARGET_STATUS: Final[Mapping[ExecutionEventType, OrderStatus | None]] = {
    ExecutionEventType.ACCEPTED: OrderStatus.SUBMITTED,
    ExecutionEventType.REJECTED: OrderStatus.REJECTED,
    ExecutionEventType.PARTIAL_FILL: OrderStatus.PARTIALLY_FILLED,
    ExecutionEventType.FILL: OrderStatus.FILLED,
    ExecutionEventType.CANCELLED: OrderStatus.CANCELLED,
    ExecutionEventType.ERROR: None,
}

#: Current statuses from which each event type may be applied.
EVENT_ALLOWED_CURRENT: Final[Mapping[ExecutionEventType, frozenset[OrderStatus]]] = {
    ExecutionEventType.ACCEPTED: frozenset({OrderStatus.CREATED}),
    ExecutionEventType.REJECTED: frozenset({OrderStatus.CREATED}),
    ExecutionEventType.PARTIAL_FILL: frozenset(
        {OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}
    ),
    ExecutionEventType.FILL: frozenset({OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}),
    ExecutionEventType.CANCELLED: frozenset(
        {OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED}
    ),
    ExecutionEventType.ERROR: frozenset(
        {
            OrderStatus.CREATED,
            OrderStatus.SUBMITTED,
            OrderStatus.PARTIALLY_FILLED,
        }
    ),
}


class OrderStateMachine:
    """Validator over the legal-transition whitelist of the order lifecycle.

    The default instance :data:`ORDER_STATE_MACHINE` uses the documented
    whitelist; the class exists so tests (and only tests) can prove the
    machinery against arbitrary whitelists.
    """

    def __init__(
        self, legal_transitions: frozenset[tuple[OrderStatus, OrderStatus]]
    ) -> None:
        self._legal_transitions = frozenset(legal_transitions)

    @property
    def legal_transitions(self) -> frozenset[tuple[OrderStatus, OrderStatus]]:
        """The full whitelist of legal ``(current, target)`` transitions."""
        return self._legal_transitions

    def is_legal(self, current: OrderStatus, target: OrderStatus) -> bool:
        """Return ``True`` iff ``current -> target`` is on the whitelist."""
        return (current, target) in self._legal_transitions

    def is_terminal(self, status: OrderStatus) -> bool:
        """Return ``True`` iff ``status`` has no outgoing legal transition."""
        return not any(
            current == status for current, _target in self._legal_transitions
        )

    def require_legal(
        self,
        current: OrderStatus,
        target: OrderStatus,
        event_type: ExecutionEventType | None = None,
    ) -> OrderStatus:
        """Validate an explicit transition, returning ``target``.

        Raises :class:`IllegalOrderTransitionError` when the transition is
        not on the whitelist.
        """
        if not self.is_legal(current, target):
            raise IllegalOrderTransitionError(current, target, event_type)
        return target

    def validate_event(
        self, current_status: OrderStatus, event_type: ExecutionEventType
    ) -> OrderStatus:
        """Validate ``event_type`` against ``current_status``.

        Returns the status the order holds after the event (unchanged for
        ``ERROR``); raises :class:`EventStateMismatchError` when the event
        cannot be applied — including any event on a terminal status.
        """
        if current_status not in EVENT_ALLOWED_CURRENT[event_type]:
            target = EVENT_TARGET_STATUS[event_type]
            effective = current_status if target is None else target
            raise EventStateMismatchError(current_status, effective, event_type)
        target = EVENT_TARGET_STATUS[event_type]
        # ``target is current`` is "stay" semantics (a later PARTIAL_FILL on an
        # already PARTIALLY_FILLED order): the status does not change, so this
        # is not a transition and the whitelist is not consulted.
        if target is None or target is current_status:
            return current_status
        return self.require_legal(current_status, target, event_type)


#: The shared order state machine instance used by every channel.
ORDER_STATE_MACHINE: Final[OrderStateMachine] = OrderStateMachine(LEGAL_TRANSITIONS)


def validate_event_for_order(event: ExecutionEvent, order: Order) -> OrderStatus:
    """Fully validate ``event`` against the current ``order`` snapshot.

    Checks, in order:

    1. the event belongs to this order (``order_id`` match);
    2. the event type is applicable to the order's current status
       (whitelist semantics above);
    3. for fill events, the payload is consistent with the order book:
       matching side/symbol and cumulative quantity rules — a
       ``PARTIAL_FILL`` must leave a remainder, a ``FILL`` must complete the
       order exactly.

    Returns the status the order holds after the event; raises
    :class:`ExecutionStateError` subclasses otherwise.
    """
    if event.order_id != order.order_id:
        raise ExecutionStateError(
            f"event order_id {event.order_id!r} does not match order {order.order_id!r}"
        )

    next_status = ORDER_STATE_MACHINE.validate_event(order.status, event.event_type)

    if event.fill is not None:
        fill = event.fill
        if fill.symbol != order.symbol:
            raise FillConsistencyError(
                f"fill symbol {fill.symbol!r} does not match order symbol {order.symbol!r}"
            )
        if fill.side is not order.side:
            raise FillConsistencyError(
                f"fill side {fill.side.value!r} does not match order side {order.side.value!r}"
            )
        cumulative = order.filled_quantity + fill.quantity
        if cumulative > order.quantity:
            raise FillConsistencyError(
                f"fill of {fill.quantity} would overfill order "
                f"{order.order_id!r}: {order.filled_quantity} + {fill.quantity} > "
                f"{order.quantity}"
            )
        if event.event_type is ExecutionEventType.PARTIAL_FILL and cumulative == order.quantity:
            raise FillConsistencyError(
                f"PARTIAL_FILL completing order {order.order_id!r} must be reported as FILL"
            )
        if event.event_type is ExecutionEventType.FILL and cumulative != order.quantity:
            raise FillConsistencyError(
                f"FILL must complete order {order.order_id!r} exactly: "
                f"{order.filled_quantity} + {fill.quantity} != {order.quantity}"
            )

    return next_status


def advance_order(order: Order, event: ExecutionEvent) -> Order:
    """Apply ``event`` to ``order`` and return the next immutable snapshot.

    Performs full :func:`validate_event_for_order` validation first, so an
    illegal event never mutates the lifecycle. Fill events accumulate the
    filled quantity and maintain the volume-weighted average fill price;
    ``REJECTED`` propagates the event's reason into ``reject_reason``.

    Channels build their progress reporting on top of this function so the
    transition semantics cannot diverge between backtest, paper and live.
    """
    next_status = validate_event_for_order(event, order)

    updates: dict[str, object] = {"status": next_status, "updated_at": event.ts}
    if event.fill is not None:
        previous_value = (
            order.avg_fill_price * order.filled_quantity if order.avg_fill_price else 0.0
        )
        filled_quantity = order.filled_quantity + event.fill.quantity
        updates["filled_quantity"] = filled_quantity
        updates["avg_fill_price"] = (
            (previous_value + event.fill.price * event.fill.quantity) / filled_quantity
            if filled_quantity
            else None
        )
    if event.event_type is ExecutionEventType.REJECTED:
        updates["reject_reason"] = event.reason

    return order.__class__(**{**order.model_dump(), **updates})


# Guard: the event tables and the transition whitelist must stay consistent —
# every (allowed current, target) pair implied by an event is a legal transition.
# Events whose target equals the current status are "stay" semantics (e.g. a
# later PARTIAL_FILL on an already PARTIALLY_FILLED order): the status does not
# change, so no whitelist entry exists or is required.
for _event_type, _allowed in EVENT_ALLOWED_CURRENT.items():
    _target = EVENT_TARGET_STATUS[_event_type]
    if _target is None:
        continue
    for _current in _allowed:
        if _current is _target:
            continue
        assert (_current, _target) in LEGAL_TRANSITIONS, (
            f"event table inconsistent with whitelist: {_event_type} implies "
            f"{_current} -> {_target} which is not legal"
        )
