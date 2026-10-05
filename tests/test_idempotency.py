"""Idempotency-key manager tests: same key -> same OrderId, conflicts, concurrency."""

from __future__ import annotations

import threading

import pytest
from pulsar_contracts import IdempotencyKey, OrderId, Side, TimeInForce

from pulsar_exec import (
    IdempotencyConflictError,
    IdempotencyManager,
    SubmitRegistration,
    default_order_id_factory,
)
from conftest import make_intent


class TestDuplicateSubmitReturnsSameOrderId:
    def test_first_submission_creates(self, intent):
        manager = IdempotencyManager()
        registration = manager.register(intent)
        assert isinstance(registration, SubmitRegistration)
        assert registration.created is True
        assert registration.order_id
        assert len(manager) == 1
        assert intent.idempotency_key in manager

    def test_resubmit_returns_same_order_id(self, intent):
        manager = IdempotencyManager()
        first = manager.register(intent)
        again = manager.register(intent)
        assert again.order_id == first.order_id
        assert again.created is False
        assert first.created is True
        assert len(manager) == 1  # no duplicate order was created

    def test_many_replays_never_duplicate(self, intent):
        manager = IdempotencyManager()
        order_ids = {manager.register(intent).order_id for _ in range(10)}
        assert len(order_ids) == 1
        assert len(manager) == 1

    def test_equal_keys_from_different_instances_bind_identically(self):
        """Two separately-built keys with equal fields are the same key."""
        manager = IdempotencyManager()
        first = manager.register(make_intent(run_id="run-x", seq=7))
        replay = manager.register(make_intent(run_id="run-x", seq=7))
        assert replay.order_id == first.order_id
        assert replay.created is False

    def test_lookup_helpers(self, intent):
        manager = IdempotencyManager()
        registration = manager.register(intent)
        assert manager.order_id_of(intent.idempotency_key) == registration.order_id
        assert manager.intent_of(intent.idempotency_key) == intent
        assert manager.order_id_of(IdempotencyKey(run_id="missing", seq=1)) is None
        assert IdempotencyKey(run_id="missing", seq=1) not in manager


class TestKeyCollisions:
    def test_different_sequence_different_order(self):
        manager = IdempotencyManager()
        a = manager.register(make_intent(seq=1))
        b = manager.register(make_intent(seq=2))
        assert a.order_id != b.order_id
        assert a.created and b.created
        assert len(manager) == 2

    def test_different_run_different_order(self):
        manager = IdempotencyManager()
        a = manager.register(make_intent(run_id="run-a", seq=1))
        b = manager.register(make_intent(run_id="run-b", seq=1))
        assert a.order_id != b.order_id
        assert len(manager) == 2

    def test_canonical_wire_form_stays_unambiguous(self):
        """"a:1" + seq 1 and "a" + seq 11 must not collide ("a:1:1" vs "a:11")."""
        manager = IdempotencyManager()
        a = manager.register(make_intent(run_id="a:1", seq=1))
        b = manager.register(make_intent(run_id="a", seq=11))
        assert a.order_id != b.order_id
        assert len(manager) == 2


class TestConflictingReuse:
    @pytest.mark.parametrize(
        "mutate",
        [
            lambda i: i.model_copy(update={"quantity": i.quantity + 100}),
            lambda i: i.model_copy(update={"symbol": "000001"}),
            lambda i: i.model_copy(update={"limit_price": (i.limit_price or 0.0) + 1.0}),
            lambda i: i.model_copy(update={"time_in_force": TimeInForce.GTC}),
        ],
    )
    def test_same_key_different_intent_rejected(self, intent, mutate):
        manager = IdempotencyManager()
        manager.register(intent)
        diverged = mutate(intent)
        with pytest.raises(IdempotencyConflictError) as excinfo:
            manager.register(diverged)
        assert intent.idempotency_key.to_str() in str(excinfo.value)
        assert excinfo.value.existing == intent
        assert excinfo.value.incoming == diverged

    def test_side_flip_rejected(self):
        manager = IdempotencyManager()
        manager.register(make_intent())
        flipped = make_intent(side=Side.SELL)  # same default key, opposite side
        with pytest.raises(IdempotencyConflictError):
            manager.register(flipped)

    def test_conflict_leaves_registry_consistent(self, intent):
        manager = IdempotencyManager()
        first = manager.register(intent)
        with pytest.raises(IdempotencyConflictError):
            manager.register(intent.model_copy(update={"quantity": 999}))
        # original binding untouched: replay still returns the first order id
        assert manager.register(intent).order_id == first.order_id
        assert len(manager) == 1


class TestOrderIdFactory:
    def test_default_factory_generates_distinct_ids(self):
        ids = {default_order_id_factory() for _ in range(100)}
        assert len(ids) == 100

    def test_injected_factory_is_honored(self):
        counter = iter(f"ord-{n}" for n in range(1, 100))
        manager = IdempotencyManager(order_id_factory=lambda: OrderId(next(counter)))
        first = manager.register(make_intent(seq=1))
        second = manager.register(make_intent(seq=2))
        assert first.order_id == OrderId("ord-1")
        assert second.order_id == OrderId("ord-2")

    def test_default_binding_is_deterministic_across_managers(self):
        """Reruns rebuild identical order ids from the same idempotency key.

        The core-engine reproducibility promise (same manifest -> identical
        成交明细) extends to the event trail, and events key their fills by
        order id: two fresh managers must therefore assign the exact same
        id to the same key — no uuid4 drift between runs.
        """
        first = IdempotencyManager().register(make_intent(run_id="run-r", seq=3))
        second = IdempotencyManager().register(make_intent(run_id="run-r", seq=3))
        assert first.order_id == second.order_id

    def test_deterministic_ids_keep_shape_and_separate_keys(self):
        one = IdempotencyManager().register(make_intent(run_id="run-r", seq=1))
        other = IdempotencyManager().register(make_intent(run_id="run-r", seq=2))
        assert one.order_id.startswith("ord-") and len(one.order_id) == 36
        assert one.order_id != other.order_id


class TestConcurrency:
    def test_concurrent_identical_submissions_yield_single_order(self):
        manager = IdempotencyManager()
        intent = make_intent()
        workers = 16
        barrier = threading.Barrier(workers)
        results: list[SubmitRegistration] = []
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            registration = manager.register(intent)
            with lock:
                results.append(registration)

        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(results) == workers
        assert len({r.order_id for r in results}) == 1
        assert sum(1 for r in results if r.created) == 1
        assert len(manager) == 1

    def test_concurrent_distinct_keys_register_all(self):
        manager = IdempotencyManager()
        workers = 8
        barrier = threading.Barrier(workers)
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker(seq: int) -> None:
            barrier.wait()
            try:
                for _ in range(2):  # each key submitted twice concurrently
                    manager.register(make_intent(seq=seq))
            except BaseException as exc:  # noqa: BLE001 - recorded for assertion
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(seq,)) for seq in range(1, workers + 1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert len(manager) == workers
