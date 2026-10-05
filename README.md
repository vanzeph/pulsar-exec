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
- **BacktestVenue (training channel)** — an event-driven, bar-level
  matching engine implementing the full `ExecutionPort` contract: A-share
  fillability rules (price-limit bands, sealed one-line boards,
  suspensions, volume participation caps, strict boundary mode),
  configurable fee and slippage models, and a cash account with T+1
  sellable positions.

## Scope

- **Backtest venue delivered; paper broker and live gateways** (paper
  ledger, miniQMT live gateway, reconciliation) arrive in later
  milestones.
- Depends only on `pulsar-contracts` (pinned git reference). No channel
  SDK, no I/O beyond injected bars, no credentials.

## Installation

```bash
pip install .
# development
pip install -e .[dev]
```

Python >= 3.11.

## Usage

### Order semantics

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

### Backtest venue

```python
from datetime import date, datetime

from pulsar_contracts import (
    Board, Exchange, Freq, IdempotencyKey, Instrument,
    OrderIntent, PriceMode, Side,
)
from pulsar_exec import BacktestVenue, SlippageModel

instrument = Instrument(symbol="600519", exchange=Exchange.SSE,
                        board=Board.MAIN, list_date=date(2001, 8, 27))
venue = BacktestVenue(
    initial_cash=1_000_000.0,
    instruments=[instrument],
    slippage=SlippageModel(fixed_bps=5.0),   # conservative default
    clock=datetime(2026, 10, 5, 9, 30),
)
venue.on_event(lambda event: print(event.event_type, event.fill or event.reason))

order_id = venue.submit(OrderIntent(
    idempotency_key=IdempotencyKey(run_id="run-42", seq=1),
    side=Side.BUY, symbol="600519", quantity=1000,
    price_mode=PriceMode.LIMIT, limit_price=1800.0,
))

for bar in historical_bars:        # replay loop feeds the venue
    venue.on_bar(bar)
venue.on_session_end(date(2026, 10, 5))   # expire DAY orders, roll T+1

print(venue.query(order_id), venue.positions(), venue.cash)
```

**Fillability.** Limit orders fill when the bar's low (high, for sells)
touches or crosses the limit — strict penetration required in strict
mode — at the better of open and limit; marketable intents
(counter-price / five-level-IOC) fill at the bar close, the IOC
cancelling its remainder on the same bar. Orders collectively consume at
most `max_volume_fraction` (default 10%) of a bar's volume. Sealed
one-line limit boards (一字板) and suspended symbols never fill.

**A-share rules.** Buys must be whole 100-share lots; odd lots sell only
as a one-shot sale of the whole available position; shares bought today
are not sellable until the trading day rolls (T+1); limit prices outside
the day's band (main ±10%, GEM/STAR ±20%, ST ±5%) are rejected.

**Fees & slippage (all configuration with policy defaults).**

| Component | Default | Direction |
|-|-|-|
| commission | 0.0003, min 5.00 CNY per trade | buy + sell |
| stamp duty | 0.0005 | sell only |
| transfer fee | 0.00001 | buy + sell |
| slippage | 5 bps fixed + optional volume-impact term | against you |

Fees are booked per fill in exact decimal arithmetic, rounded to the cent
(ROUND_HALF_UP), and carried on the `Fill` payload.

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
