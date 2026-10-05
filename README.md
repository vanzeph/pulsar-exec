# pulsar-exec

The execution layer of the **Pulsar** A-share quantitative trading system.
This repository delivers the execution-side semantics shared by every
channel (backtest venue, paper broker, live broker gateway) on top of
[`pulsar-contracts`](https://github.com/vanzeph/pulsar-contracts):

- **Order state machine** — the legal-transition whitelist
  `Created -> Submitted -> PartiallyFilled -> Filled / Cancelled / Rejected`
  with rejection of every illegal transition, plus event-driven advancement
  of immutable order snapshots.
- **Execution events** — canonical constructors for every
  `ExecutionEvent` kind (`accepted`, `rejected`, `partial_fill`, `fill`,
  `cancelled`, `error`) with full payload and fill-arithmetic validation.
- **Idempotency** — key management for `run id + sequence` order intents:
  re-submitting the same key returns the same `OrderId`; re-using a key for
  a different intent is a rejected conflict. Retries and replays never
  produce duplicate orders at the venue.

## Scope

- **Semantics and validation only.** Concrete venues/brokers/gateways
  (backtest matcher, paper broker, miniQMT live gateway, reconciliation)
  are delivered in later milestones.
- Depends only on `pulsar-contracts` (pinned git reference). No channel
  SDK, no I/O, no configuration handling.

## Installation

```bash
pip install .
# development
pip install -e .[dev]
```

Python >= 3.11.

## Usage

```python
from datetime import datetime

from pulsar_contracts import IdempotencyKey, OrderIntent, PriceMode, Side
from pulsar_exec import (
    IdempotencyManager,
    accepted,
    advance_order,
    partial_fill,
    fill,
    validate_event_for_order,
)
from pulsar_contracts import Fill, OrderStatus

manager = IdempotencyManager()

intent = OrderIntent(
    idempotency_key=IdempotencyKey(run_id="run-42", seq=1),
    side=Side.BUY,
    symbol="600519",
    quantity=300,
    price_mode=PriceMode.LIMIT,
    limit_price=1800.0,
)

registration = manager.register(intent)          # creates ord-...
replay = manager.register(intent)                # same OrderId, created=False

order = advance_order(
    order,  # your Order snapshot in Created
    accepted(registration.order_id, datetime.now()),
)
order = advance_order(
    order,
    partial_fill(
        registration.order_id,
        datetime.now(),
        Fill(fill_id="f1", order_id=registration.order_id, symbol="600519",
             side=Side.BUY, price=1799.0, quantity=100, ts=datetime.now()),
    ),
)
assert order.status is OrderStatus.PARTIALLY_FILLED
```

Any transition outside the whitelist raises `IllegalOrderTransitionError`
(or its `EventStateMismatchError` subtype when driven by an event), and
fill events that would overfill an order, complete it as a `PARTIAL_FILL`,
or stop short of completion as a `FILL` raise `FillConsistencyError`.

## State machine

```
Created ----> Submitted ----> PartiallyFilled ----> Filled
   |             |                   |
   v             v                   v
Rejected     Cancelled           Cancelled
```

`Filled`, `Cancelled` and `Rejected` are terminal. The `ERROR` event maps
to no transition: unconfirmed orders keep their status and converge via
`query`/reconciliation, never by assuming failure.

## Development

```bash
pip install -e .[dev]
pytest
mypy
```
