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
- **Live channel (miniQMT first delivery)** — `MiniQMTGateway`, a live
  `ExecutionPort` implementation against a local `BrokerSession`
  protocol/stub (the real `xtquant` SDK is referenced only by a dynamic,
  terminal-bound bridge); `LiveGate` (unlock environment variable,
  per-order and daily value caps, append-only rejection trail);
  reconciliation (post-market report + reconnect converge-first resume
  with a read-only alert on differences); `safe_shutdown` (cancel active
  orders, refuse new intents, persist the shutdown manifest); and a JSONL
  event archive for audit.

## Scope

- **Backtest venue and the live channel delivered; the paper broker**
  arrives in a later milestone.
- The live channel's broker-facing behaviour is fully unit-tested against
  the in-memory protocol fake. The broker simulation drill
  (buy/sell/cancel/reconcile round trip on a miniQMT terminal) requires
  the terminal and a simulation account and is executed on
  terminal-equipped machines — see *Live gateway* below.
- Depends only on `pulsar-contracts` (pinned git reference). No channel
  SDK import at module load, no I/O beyond injected sessions, no
  credentials in source (environment variable names only).

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

### Live gateway (miniQMT)

```python
from pulsar_exec import LiveGate, LiveGateConfig, MiniQMTGateway, safe_shutdown
from pulsar_exec.live import run_post_market_reconciliation
from pulsar_exec.live.xt_bridge import open_broker_session

session = open_broker_session()   # reads PULSAR_MINIQMT_ACCOUNT_ID etc.
gateway = MiniQMTGateway(
    session=session,
    gate=LiveGate(LiveGateConfig(
        unlock_env="PULSAR_LIVE_CONFIRM",   # live stays locked without it
        max_order_value=50_000.0,           # per-order cap (CNY)
        max_daily_traded_value=500_000.0,   # daily cumulative cap (CNY)
    )),
    run_id="run-live-0001",
)
gateway.on_event(lambda event: print(event.event_type, event.fill or event.reason))
gateway.start()

order_id = gateway.submit(intent)   # refused unless unlocked and within caps
...
report = run_post_market_reconciliation(gateway, "reports/eod.json")
manifest = safe_shutdown(gateway, reason="session end",
                         manifest_dir="runs/run-live-0001/")
```

**Gate (实盘门禁).** Live is locked by default: the unlock environment
variable must be explicitly set (optionally to a pinned token) before any
order reaches the broker. Each intent's notional
(`reference price x quantity`) must fit the per-order cap and the daily
cumulative traded-value budget; violations — plus a halted gate, or an
intent that cannot be priced — are refused with a `REJECTED` event and an
append-only JSONL rejection trail (`LiveGate.write_rejection_log`).

**Unconfirmed outcomes.** A submission or cancellation that cannot be
confirmed (session down) emits `ERROR`, leaves the order state untouched
and converges via `poll()`/reconciliation; it is never assumed failed and
re-sent.

**Reconciliation (对账).** `gateway.reconcile()` first converges the local
book onto broker truth (fills that happened while disconnected), then
diffs orders, trades, positions and (optionally) cash. Any difference
produces a report entry, keeps the gateway in a read-only alert that
refuses new intents, and blocks the next Live start until an operator
acknowledges (`gateway.acknowledge_alert(note)`). On reconnect the same
machinery runs *before* the gateway resumes accepting intents.

**Safe shutdown (安全停机).** `safe_shutdown` halts new intents, cancels
every active order (recording — never assuming — unconfirmed cancels),
converges via reconciliation when the session allows, disconnects and
writes a JSON manifest of the final state.

**Environment variables** (names only ever appear in source):
`PULSAR_LIVE_CONFIRM` (gate unlock), `PULSAR_MINIQMT_ACCOUNT_ID`,
`PULSAR_MINIQMT_USERDATA`, `PULSAR_MINIQMT_SESSION_ID` (bridge). The
`xtquant` SDK is an optional dependency (`pip install .[live]`) — it
ships with the miniQMT terminal and is loaded dynamically by the bridge,
so everything else works (and tests run) without it.

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
