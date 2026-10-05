"""Shared fixtures and builders for pulsar-exec tests."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pulsar_contracts import (
    Fill,
    IdempotencyKey,
    Order,
    OrderId,
    OrderIntent,
    OrderStatus,
    PriceMode,
    Side,
)

TEST_RUN_ID = "run-20261005-0001"


def make_intent(
    seq: int = 1,
    side: Side = Side.BUY,
    symbol: str = "600519",
    quantity: int = 300,
    limit_price: float | None = 1800.0,
    run_id: str = TEST_RUN_ID,
) -> OrderIntent:
    """Build a valid limit OrderIntent with the given idempotency sequence."""
    return OrderIntent(
        idempotency_key=IdempotencyKey(run_id=run_id, seq=seq),
        side=side,
        symbol=symbol,
        quantity=quantity,
        price_mode=PriceMode.LIMIT,
        limit_price=limit_price,
    )


def make_fill(
    order_id: OrderId,
    price: float,
    quantity: int,
    side: Side = Side.BUY,
    symbol: str = "600519",
    seq: int = 1,
) -> Fill:
    return Fill(
        fill_id=f"fill-{seq}",
        order_id=order_id,
        symbol=symbol,
        side=side,
        price=price,
        quantity=quantity,
        ts=make_ts(minutes=seq),
    )


def make_ts(minutes: int = 0) -> datetime:
    """Naive Shanghai wall time; contracts normalize to Asia/Shanghai."""
    return datetime(2026, 10, 5, 9, 30, 0) + timedelta(minutes=minutes)


def make_order(
    order_id: OrderId | None = None,
    intent: OrderIntent | None = None,
    status: OrderStatus = OrderStatus.CREATED,
    filled_quantity: int = 0,
    avg_fill_price: float | None = None,
    reject_reason: str | None = None,
) -> Order:
    """Build an Order snapshot in the given state."""
    intent = intent or make_intent()
    base = Order.from_intent(
        order_id or OrderId("ord-test-1"), intent, created_at=make_ts()
    )
    return base.model_copy(
        update={
            "status": status,
            "filled_quantity": filled_quantity,
            "avg_fill_price": avg_fill_price,
            "reject_reason": reject_reason,
        }
    )


@pytest.fixture()
def intent() -> OrderIntent:
    return make_intent()
