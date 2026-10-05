"""Idempotency-key management for order submission.

An :class:`~pulsar_contracts.common.IdempotencyKey` is ``run id`` + per-run
sequence number; it travels unchanged from the core engine down to the venue
gateway. The semantics implemented here are the ones the Pulsar execution
design mandates:

* submitting the **same key with the same intent** again returns the
  **same** :class:`~pulsar_contracts.common.OrderId` — retries and replays
  never produce a duplicate order at the venue;
* submitting the **same key with a different intent** is a conflict and is
  rejected loudly instead of silently re-pointing the key;
* different keys map to different order ids.

Order-id assignment is **deterministic by default**: the id is the SHA-256
digest of the wire-form idempotency key (``ord-<first 32 hex>``), so a
rerun of the same run manifest reproduces byte-identical order trails —
the core-engine reproducibility promise covers 成交明细, and event
archives key fills by order id. Channels that prefer exchange-assigned
or random ids (the live gateway) inject their own factory.

Venues/gateways consult the manager *before* forwarding anything to the
matching engine or broker session, which is what makes ``submit``
idempotent end to end.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from hashlib import sha256
from typing import Callable

from pulsar_contracts import (
    IdempotencyKey,
    OrderId,
    OrderIntent,
)

__all__ = [
    "IdempotencyConflictError",
    "SubmitRegistration",
    "IdempotencyManager",
    "default_order_id_factory",
    "deterministic_order_id",
]


class IdempotencyConflictError(ValueError):
    """The same idempotency key was reused with a different intent."""

    def __init__(self, key: IdempotencyKey, existing: OrderIntent, incoming: OrderIntent) -> None:
        self.key = key
        self.existing = existing
        self.incoming = incoming
        super().__init__(
            f"idempotency key {key.to_str()!r} already bound to a different "
            f"intent ({existing.side.value} {existing.quantity} {existing.symbol}) "
            f"than the submitted one ({incoming.side.value} {incoming.quantity} {incoming.symbol}); "
            f"an idempotency key must identify exactly one order intent"
        )


@dataclass(frozen=True)
class SubmitRegistration:
    """Outcome of registering an intent with :class:`IdempotencyManager`.

    ``created`` is ``True`` when this registration created the order (the
    venue must forward it), ``False`` when it was a retry/replay of a key
    already registered (the venue must NOT forward it again).
    """

    order_id: OrderId
    created: bool


def deterministic_order_id(wire_key: str) -> OrderId:
    """Derive the default order id from an idempotency key's wire form.

    ``ord-<32 hex>`` — the same shape the historical uuid4-based ids had,
    but a pure function of the key: reruns of the same manifest rebuild
    the same ids, keeping event journals and fill blotters bit-identical.
    """
    return OrderId(f"ord-{sha256(wire_key.encode('utf-8')).hexdigest()[:32]}")


def default_order_id_factory() -> OrderId:
    """Generate a fresh random venue-style order id.

    Opt-in for channels that do not want key-derived ids (e.g. a live
    gateway mirroring broker-assigned identifiers); the backtest venue
    and paper broker use :func:`deterministic_order_id` instead.
    """
    return OrderId(f"ord-{uuid.uuid4().hex}")


class IdempotencyManager:
    """Registry mapping idempotency keys to exactly one order id each.

    Thread-safe: gateway retry paths and reconciliation loops may hit the
    registry concurrently. Without an injected ``order_id_factory`` the
    binding derives deterministically from the key (see
    :func:`deterministic_order_id`).
    """

    def __init__(
        self, order_id_factory: "Callable[[], OrderId] | None" = None
    ) -> None:
        self._order_id_factory = order_id_factory
        self._lock = threading.Lock()
        self._bindings: dict[str, OrderIntent] = {}
        self._order_ids: dict[str, OrderId] = {}

    def register(self, intent: OrderIntent) -> SubmitRegistration:
        """Register ``intent``; idempotent on its key.

        Returns the :class:`SubmitRegistration` carrying the order id. The
        first registration of a key assigns its order id (key-derived by
        default); re-submitting an identical intent returns the same order
        id with ``created=False``; re-using the key for a different intent
        raises :class:`IdempotencyConflictError`.
        """
        wire_key = intent.idempotency_key.to_str()
        with self._lock:
            existing = self._bindings.get(wire_key)
            if existing is not None:
                if existing != intent:
                    raise IdempotencyConflictError(
                        intent.idempotency_key, existing, intent
                    )
                return SubmitRegistration(self._order_ids[wire_key], created=False)
            order_id = (
                self._order_id_factory()
                if self._order_id_factory is not None
                else deterministic_order_id(wire_key)
            )
            self._bindings[wire_key] = intent
            self._order_ids[wire_key] = order_id
            return SubmitRegistration(order_id, created=True)

    def order_id_of(self, key: IdempotencyKey) -> OrderId | None:
        """Return the order id bound to ``key``, or ``None`` if unregistered."""
        return self._order_ids.get(key.to_str())

    def intent_of(self, key: IdempotencyKey) -> OrderIntent | None:
        """Return the intent bound to ``key``, or ``None`` if unregistered."""
        return self._bindings.get(key.to_str())

    def __contains__(self, key: IdempotencyKey) -> bool:
        return key.to_str() in self._order_ids

    def __len__(self) -> int:
        return len(self._order_ids)
