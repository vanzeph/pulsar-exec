"""Append-only JSONL archive of execution events (订单事件与回报落盘).

The Pulsar execution design requires every order event and broker report to
be persisted so any run can be replayed and audited order by order.  The
live gateway is constructed with an optional :class:`EventArchive`; every
event it emits (accepted / rejected / fills / cancels / unconfirmed errors)
is appended as one JSON line before the callbacks see it.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from pulsar_contracts import ExecutionEvent

__all__ = ["EventArchive"]


def _event_payload(event: ExecutionEvent) -> dict[str, object]:
    payload: dict[str, object] = {
        "event_type": event.event_type.value,
        "order_id": str(event.order_id),
        "ts": event.ts.isoformat(),
        "reason": event.reason,
    }
    if event.fill is not None:
        payload["fill"] = event.fill.model_dump(mode="json")
    return payload


class EventArchive:
    """Thread-safe JSON-lines sink for execution events of one live run."""

    def __init__(self, directory: Path | str, *, filename: str = "events.jsonl") -> None:
        self._path = Path(directory) / filename
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        """The JSONL file events are appended to."""
        return self._path

    def append(self, event: ExecutionEvent) -> None:
        """Append one event; crashes propagate (an unauditable run stops)."""
        line = json.dumps(_event_payload(event), ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    def read_lines(self) -> list[dict[str, object]]:
        """Read the archive back (audit helper; empty list when absent)."""
        if not self._path.exists():
            return []
        rows: list[dict[str, object]] = []
        with self._lock, self._path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        return rows
