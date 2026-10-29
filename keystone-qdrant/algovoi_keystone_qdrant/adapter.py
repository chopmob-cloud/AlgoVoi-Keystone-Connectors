"""algovoi-keystone-qdrant -- bind Qdrant vector writes to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). Agent memory is a data plane: when a keystone decision
authorises a write to the vector store, this binds each upsert / delete / payload write to that
decision, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``qdrant:upsert`` / ``:delete`` / ``:set_payload`` / ``:overwrite_payload``
and ``scope`` is the collection name. ``qdrant_execution_ref`` is the standalone helper;
``keystone_qdrant_client`` wraps any qdrant-client ``QdrantClient`` so each write emits an
``execution_ref``, with reads (search, retrieve, scroll, query, ...) passing through. No
qdrant-client dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneQdrantError",
    "EXECUTION_OUTCOMES",
    "qdrant_execution_ref",
    "KeystoneQdrantClient",
    "keystone_qdrant_client",
    "tamper_detected",
]

_ACTIONS = ("upsert", "delete", "set_payload", "overwrite_payload")


class KeystoneQdrantError(ValueError):
    """Raised on invalid binding inputs."""


def qdrant_execution_ref(*, decision_ref: str, collection: str, outcome: str,
                         executed_at_ms: int, action: str = "upsert") -> str:
    """Bind one Qdrant vector write to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneQdrantError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if action not in _ACTIONS:
        raise KeystoneQdrantError(f"action must be one of {_ACTIONS}")
    if not (isinstance(collection, str) and collection):
        raise KeystoneQdrantError("collection must be a non-empty string")
    return execution_ref(decision_ref, f"qdrant:{action}", collection, outcome, executed_at_ms)


class KeystoneQdrantClient:
    """Wrap a ``QdrantClient`` so each vector write emits a keystone execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The scope is the
    call's ``collection_name``. ``execution_ref`` holds the last binding; ``executions`` the full
    list. A write that raises is recorded ``FAILED`` and the error re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneQdrantError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, action: str) -> Callable:
        fn = getattr(self._c, action)

        def caller(*args: Any, **kwargs: Any) -> Any:
            collection = kwargs.get("collection_name") or (args[0] if args else None)
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record(action, collection, "FAILED", ts)
                raise
            self._record(action, collection, "COMMITTED", ts)
            return result

        return caller

    def _record(self, action: str, collection: Any, outcome: str, ts: int) -> None:
        col = str(collection or "")
        ref = qdrant_execution_ref(decision_ref=self._decision_ref, collection=col,
                                   outcome=outcome, executed_at_ms=ts, action=action)
        rec = {"decision_ref": self._decision_ref, "scope": col,
               "action_type": f"qdrant:{action}", "collection": collection,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        if name in _ACTIONS:
            return self._wrap(name)
        return getattr(self._c, name)


def keystone_qdrant_client(client: Any, *, decision_ref: str,
                           clock_ms: Callable[[], int] | None = None,
                           on_execution: Callable[[dict], None] | None = None) -> KeystoneQdrantClient:
    """Wrap a ``QdrantClient`` so each vector write is bound to ``decision_ref``."""
    return KeystoneQdrantClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                                on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, collection: str, outcome: str,
                    executed_at_ms: int, action: str = "upsert") -> bool:
    """Return True if the write's fields do not recompute to the claimed execution_ref."""
    try:
        return qdrant_execution_ref(decision_ref=decision_ref, collection=collection,
                                    outcome=outcome, executed_at_ms=executed_at_ms,
                                    action=action) != claimed_ref
    except Exception:
        return True
